"""
Latency of POST /nl2sql, measured on the 20 gold questions (one request each, sequential).
Reports the server-side split: LLM time (Claude call, plus one retry if needed) vs SQL
execution time, and end-to-end time measured by the client.

Needs the API running and ANTHROPIC_API_KEY in .env:
  python loadtest/nl2sql_latency.py --label shared
"""
import argparse, json, statistics, time
import requests

p = argparse.ArgumentParser()
p.add_argument("--label", required=True)
p.add_argument("--host", default="http://127.0.0.1:8000")
p.add_argument("--questions", default="eval/gold.json")
args = p.parse_args()


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round(q * (len(xs) - 1))))], 1)


rows = []
for item in json.load(open(args.questions)):
    t0 = time.perf_counter()
    r = requests.post(f"{args.host}/nl2sql", json={"question": item["question"]}, timeout=120)
    total = (time.perf_counter() - t0) * 1000
    body = r.json()
    t = body.get("timing_ms", {}) if r.ok else {}
    rows.append({"id": item["id"], "status": r.status_code, "total_ms": round(total, 1),
                 "llm_ms": t.get("llm"), "execute_ms": t.get("execute"),
                 "retried": body.get("retried") if r.ok else None})
    print(rows[-1])

ok = [x for x in rows if x["status"] == 200]
summary = {"label": args.label, "requests": len(rows), "ok": len(ok)}
for k in ("total_ms", "llm_ms", "execute_ms"):
    xs = [x[k] for x in ok]
    summary[k] = {"p50": pct(xs, 0.5), "p95": pct(xs, 0.95), "max": round(max(xs), 1)} if xs else None
print(json.dumps(summary, indent=2))
json.dump({"summary": summary, "requests": rows},
          open(f"results/api/nl2sql_latency_{args.label}.json", "w"), indent=2)
