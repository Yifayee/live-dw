"""
Replay Twitch viewing sessions into Kafka as 10-minute heartbeat events.

Each row of dwd_watch_session (user, stream, start_step, stop_step) becomes one
heartbeat per step in [start_step, stop_step), which mirrors how the source data
was collected (chat crawled every 10 minutes).

Optional controlled fault injection (seeded, logged to a ground-truth file):
  --late-rate   share of events emitted k steps after their event time (k in 1..max-delay)
  --dup-rate    share of events emitted a second time with the same event_id

Run from the live-dw root, for example:
  python scripts/stream_replay_twitch.py --start-day 10 --end-day 16 --dry-run
  python scripts/stream_replay_twitch.py --start-day 10 --end-day 16 --late-rate 0.01 --dup-rate 0.005

After the last step, one flush marker (user_id = -1, far-future event time) is sent so a
streaming consumer's watermark moves past the final windows and closes them. Consumers
must exclude it from their metrics, but only after the watermark has seen it.
Disable with --no-flush.
"""
import argparse
import csv
import json
import random
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

STEP_MS = 10 * 60 * 1000
STEPS_PER_DAY = 144
# Fixed, 10-minute-aligned base so that step s maps to event time BASE + s * 10 min
# and Spark's 10-minute tumbling windows line up exactly with steps.
BASE_MS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--start-day", type=int, required=True)
    p.add_argument("--end-day", type=int, required=True, help="inclusive")
    p.add_argument("--topic", default="twitch_heartbeats")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--late-rate", type=float, default=0.0)
    p.add_argument("--max-delay", type=int, default=6, help="max delay in steps")
    p.add_argument("--dup-rate", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--step-seconds", type=float, default=0.0,
                   help="wall-clock seconds per 10-minute step; 0 = as fast as possible")
    p.add_argument("--run-id", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    p.add_argument("--dry-run", action="store_true", help="build events but do not send to Kafka")
    p.add_argument("--out-dir", help="write one JSON-lines file per step here instead of Kafka (local testing)")
    p.add_argument("--no-flush", action="store_true", help="do not send the end-of-stream flush marker")
    return p.parse_args()


def load_heartbeats(db_path, start_day, end_day):
    lo = start_day * STEPS_PER_DAY
    hi = (end_day + 1) * STEPS_PER_DAY
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute(
        """
        with sessions as (
            select user_id, stream_id, streamer, start_step, stop_step
            from dwd_watch_session
            where start_step < ? and stop_step > ?
        ),
        beats as (
            select user_id, stream_id, streamer,
                   unnest(range(start_step, stop_step)) as step
            from sessions
        )
        select user_id, stream_id, streamer, step
        from beats
        where step >= ? and step < ?
        order by step
        """,
        [hi, lo, lo, hi],
    ).fetchall()
    con.close()
    return rows, lo, hi


def make_producer(bootstrap):
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 1_000_000,
    })


def send(producer, topic, event):
    if producer is None:
        return
    payload = json.dumps(event).encode()
    while True:
        try:
            # No record timestamp: Kafka stamps produce time. Event time stays in the payload.
            # (Historical event times as record timestamps make Kafka's 7-day retention
            # delete the data almost immediately.)
            producer.produce(topic, key=str(event["user_id"]).encode(), value=payload)
            break
        except BufferError:
            producer.poll(0.2)  # local queue full: let it drain, then retry
    producer.poll(0)


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    t0 = time.time()
    rows, lo, hi = load_heartbeats(args.db, args.start_day, args.end_day)
    load_sec = time.time() - t0
    print(f"Loaded {len(rows):,} heartbeats for steps [{lo}, {hi}) in {load_sec:.1f} s")

    by_step = defaultdict(list)
    for user_id, stream_id, streamer, step in rows:
        by_step[step].append((user_id, stream_id, streamer, step))

    out_dir = Path("data/stream")
    out_dir.mkdir(parents=True, exist_ok=True)
    truth_path = out_dir / f"injections_{args.run_id}.csv"
    truth_f = open(truth_path, "w", newline="")
    truth = csv.writer(truth_f)
    truth.writerow(["event_id", "step", "kind", "delay_steps", "emit_step"])

    file_dir = Path(args.out_dir) if args.out_dir else None
    if file_dir:
        file_dir.mkdir(parents=True, exist_ok=True)
    producer = None if (args.dry_run or file_dir) else make_producer(args.bootstrap)

    def emit(batch, tag):
        if file_dir:
            with open(file_dir / f"{tag}.json", "w") as f:
                for event in batch:
                    f.write(json.dumps(event) + "\n")
        else:
            for event in batch:
                send(producer, args.topic, event)

    pending = defaultdict(list)  # emit_step -> events held back (late)
    counts = defaultdict(int)
    send_start = time.time()

    for emit_step in range(lo, hi + args.max_delay + 1):
        tick = time.time()
        outgoing = []

        for user_id, stream_id, streamer, step in by_step.get(emit_step, []):
            event = {
                "event_id": f"{user_id}:{stream_id}:{step}",
                "user_id": user_id,
                "stream_id": stream_id,
                "streamer": streamer,
                "step": step,
                "event_ts_ms": BASE_MS + step * STEP_MS,
            }
            if args.late_rate and rng.random() < args.late_rate:
                delay = rng.randint(1, args.max_delay)
                pending[emit_step + delay].append(event)
                truth.writerow([event["event_id"], step, "late", delay, emit_step + delay])
                counts["late"] += 1
            else:
                outgoing.append(event)
                counts["on_time"] += 1

            if args.dup_rate and rng.random() < args.dup_rate:
                outgoing.append(dict(event))
                truth.writerow([event["event_id"], step, "duplicate", 0, emit_step])
                counts["duplicate"] += 1

        outgoing.extend(pending.pop(emit_step, []))

        emit(outgoing, f"step_{emit_step:06d}")
        counts["sent"] += len(outgoing)

        if args.step_seconds > 0:
            time.sleep(max(0.0, args.step_seconds - (time.time() - tick)))

    if not args.no_flush and not args.dry_run:
        flush_step = hi + args.max_delay + 12
        emit([{"event_id": "__flush__", "user_id": -1, "stream_id": -1, "streamer": "__flush__",
               "step": flush_step, "event_ts_ms": BASE_MS + flush_step * STEP_MS}], "zz_flush")
        counts["flush"] = 1

    if producer is not None:
        producer.flush()
    send_sec = time.time() - send_start
    truth_f.close()

    summary = {
        "run_id": args.run_id,
        "dry_run": args.dry_run,
        "days": [args.start_day, args.end_day],
        "steps": [lo, hi],
        "heartbeats_source": len(rows),
        "events_sent": counts["sent"],
        "on_time": counts["on_time"],
        "late": counts["late"],
        "duplicates": counts["duplicate"],
        "flush_markers": counts["flush"],
        "late_rate": args.late_rate,
        "max_delay_steps": args.max_delay,
        "dup_rate": args.dup_rate,
        "seed": args.seed,
        "step_seconds": args.step_seconds,
        "load_seconds": round(load_sec, 2),
        "send_seconds": round(send_sec, 2),
        "events_per_second": round(counts["sent"] / send_sec) if send_sec > 0 else None,
        "truth_file": str(truth_path),
    }
    res_dir = Path("results/stream")
    res_dir.mkdir(parents=True, exist_ok=True)
    with open(res_dir / f"replay_summary_{args.run_id}.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
