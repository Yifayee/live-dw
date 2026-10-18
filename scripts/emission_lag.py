"""
Approximate output latency of the streaming job: for each 10-minute window, the
wall-clock seconds between the moment the replay finished sending that window's
on-time events and the moment Spark wrote the window's Parquet file.

Replay start is estimated as (replay summary file mtime - send_seconds); window s is
fully sent at start + (s + 1 - first_step) * step_seconds. The final window is skipped
because it is closed by the flush marker. File mtimes make this approximate (~0.1 s).

  python scripts/emission_lag.py --run-ids faulty2_d10_16 wm70_d10_16
"""
import argparse, glob, json, os, statistics
import duckdb

p = argparse.ArgumentParser()
p.add_argument("--run-ids", nargs="+", required=True)
p.add_argument("--metric", default="sessions_online")
args = p.parse_args()

out = {}
for run in args.run_ids:
    path = f"results/stream/replay_summary_{run}.json"
    r = json.load(open(path))
    lo, hi = r["steps"]
    start = os.path.getmtime(path) - r["send_seconds"]
    lags = []
    for f in glob.glob(f"data/stream/spark_out/{run}/{args.metric}/*.parquet"):
        for (s,) in duckdb.sql(f"select step from '{f}'").fetchall():
            if s < hi - 1:
                lags.append(os.path.getmtime(f) - (start + (s + 1 - lo) * r["step_seconds"]))
    lags.sort()
    out[run] = {"metric": args.metric, "windows": len(lags),
                "lag_sec_p50": round(statistics.median(lags), 1),
                "lag_sec_p95": round(lags[int(len(lags) * 0.95)], 1),
                "lag_sec_max": round(lags[-1], 1),
                "step_seconds": r["step_seconds"]}
print(json.dumps(out, indent=2))
with open(f"results/stream/emission_lag_{args.metric}_" + "_vs_".join(args.run_ids) + ".json", "w") as fh:
    json.dump(out, fh, indent=2)
