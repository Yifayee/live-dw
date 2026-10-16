"""
Real-time concurrency with Spark Structured Streaming.

Reads heartbeat events (from Kafka, or from JSON files for local testing), assigns them
to 10-minute event-time windows with a watermark, and writes three per-window metrics
as Parquet in append mode (a window is written once, after the watermark passes it):

  events_raw      every event counted, duplicates included          (no dedup)
  sessions_online distinct event_id per window = (user, stream) pairs (offline definition)
  users_online    distinct user per window                            (stricter definition)

Late events are dropped once Spark's watermark has passed their window. Batch timings and
Spark's own drop counters are saved from query progress; the exact number of dropped
events is measured later by the reconciliation script against the injection ground truth.

Run from the live-dw root while the replay script is producing, for example:
  python scripts/stream_concurrency_spark.py --topic twitch_hb_clean --run-id clean_d10_16
"""
import argparse
import json
import shutil
import statistics
import time
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession, functions as F, types as T

STEP_MS = 10 * 60 * 1000
BASE_MS = 1767225600000  # 2026-01-01T00:00:00Z, same anchor as stream_replay_twitch.py

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("stream_id", T.LongType()),
    T.StructField("streamer", T.StringType()),
    T.StructField("step", T.LongType()),
    T.StructField("event_ts_ms", T.LongType()),
])


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--topic", default="twitch_heartbeats")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--input-dir", help="JSON-lines directory when --source files")
    p.add_argument("--run-id", required=True)
    p.add_argument("--watermark-min", type=int, default=20)
    p.add_argument("--trigger-sec", type=int, default=1)
    p.add_argument("--max-offsets", type=int, default=200000, help="Kafka records per micro-batch cap")
    p.add_argument("--cores", type=int, default=4)
    p.add_argument("--idle-sec", type=int, default=30, help="stop after this long with no input")
    return p.parse_args()


def build_spark(args):
    b = (SparkSession.builder
         .appName(f"live-dw-stream-{args.run_id}")
         .master(f"local[{args.cores}]")
         .config("spark.sql.shuffle.partitions", str(args.cores))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.streaming.numRecentProgressUpdates", "100000")
         .config("spark.driver.memory", "4g")
         .config("spark.ui.showConsoleProgress", "false"))
    if args.source == "kafka":
        pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
        b = b.config("spark.jars.packages", pkg)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark, args):
    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", args.max_offsets)
               .load())
        events = raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
    else:
        events = (spark.readStream.schema(SCHEMA)
                  .option("maxFilesPerTrigger", 1)
                  .json(args.input_dir))
    # The end-of-stream flush marker (user_id = -1) must reach the watermark operator so it
    # can advance event time and close the last windows. It is NOT filtered out here:
    # Spark pushes simple filters below the watermark, which would hide the marker from it.
    # Instead the marker is excluded inside the counts (see per_window_count).
    return (events
            .withColumn("event_time", F.timestamp_millis("event_ts_ms"))
            .withWatermark("event_time", f"{args.watermark_min} minutes"))


def per_window_count(df, metric):
    return (df.groupBy(F.window("event_time", "10 minutes").alias("w"))
              .agg(F.sum(F.when(F.col("user_id") >= 0, 1).otherwise(0)).alias(metric))
              .select(((F.unix_millis(F.col("w.start")) - BASE_MS) / STEP_MS).cast("long").alias("step"),
                      F.col("w.start").alias("window_start"),
                      F.col(metric)))


def build_metrics(events):
    return {
        "events_raw": per_window_count(events, "events_raw"),
        "sessions_online": per_window_count(
            events.dropDuplicatesWithinWatermark(["event_id"]), "sessions_online"),
        "users_online": per_window_count(
            events.dropDuplicatesWithinWatermark(["user_id", "step"]), "users_online"),
    }


def summarize(progress):
    batches = [p for p in progress if p.get("numInputRows", 0) > 0]
    dur = [p["durationMs"]["triggerExecution"] for p in batches]
    dropped = sum(op.get("numRowsDroppedByWatermark", 0)
                  for p in progress for op in p.get("stateOperators", []))
    rate = [p["processedRowsPerSecond"] for p in batches if p.get("processedRowsPerSecond")]
    return {
        "input_rows": sum(p.get("numInputRows", 0) for p in progress),
        "batches_with_data": len(batches),
        "batch_ms_p50": statistics.median(dur) if dur else None,
        "batch_ms_p95": statistics.quantiles(dur, n=20)[18] if len(dur) >= 20 else None,
        "batch_ms_max": max(dur) if dur else None,
        "processed_rows_per_sec_median": round(statistics.median(rate)) if rate else None,
        # Counted by Spark's state operators after partial aggregation, so this is NOT
        # an event count. Exact late-event drops are measured in reconciliation instead.
        "state_rows_dropped_by_watermark": dropped,
    }


def main():
    args = parse_args()
    out_root = Path("data/stream/spark_out") / args.run_id
    ckpt_root = Path("data/stream/checkpoints") / args.run_id
    for d in (out_root, ckpt_root):  # a run id always starts from scratch
        shutil.rmtree(d, ignore_errors=True)

    spark = build_spark(args)
    events = read_events(spark, args)

    queries = {}
    for name, df in build_metrics(events).items():
        queries[name] = (df.writeStream
                         .format("parquet")
                         .outputMode("append")
                         .option("path", str(out_root / name))
                         .option("checkpointLocation", str(ckpt_root / name))
                         .trigger(processingTime=f"{args.trigger_sec} seconds")
                         .queryName(name)
                         .start())
    print(f"Started {len(queries)} queries; writing to {out_root}")

    started = time.time()
    seen_data = False
    last_data = time.time()
    while True:
        time.sleep(2)
        for q in queries.values():
            lp = q.lastProgress
            if lp and lp.get("numInputRows", 0) > 0:
                seen_data = True
                last_data = time.time()
        if seen_data and time.time() - last_data > args.idle_sec:
            break
        if any(q.exception() for q in queries.values()):
            raise RuntimeError([str(q.exception()) for q in queries.values() if q.exception()])

    wall = time.time() - started
    summary = {"run_id": args.run_id, "source": args.source, "topic": args.topic,
               "watermark_min": args.watermark_min, "trigger_sec": args.trigger_sec,
               "cores": args.cores, "spark_version": pyspark.__version__,
               "wall_seconds_incl_idle": round(wall, 1), "queries": {}}
    res_dir = Path("results/stream")
    res_dir.mkdir(parents=True, exist_ok=True)
    for name, q in queries.items():
        progress = q.recentProgress
        q.stop()
        summary["queries"][name] = summarize(progress)
        with open(res_dir / f"spark_progress_{args.run_id}_{name}.json", "w") as f:
            json.dump(progress, f, default=str)

    with open(res_dir / f"spark_summary_{args.run_id}.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
