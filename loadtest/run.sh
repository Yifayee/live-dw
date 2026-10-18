#!/usr/bin/env bash
# One load-test round: start the API with the given settings, run Locust, stop the API.
#   bash loadtest/run.sh <label> <DB_MODE> <CACHE> [users] [seconds]
#   bash loadtest/run.sh baseline_per_request per_request 0
# Results: results/api/<label>_stats.csv (Locust) and results/api/<label>_server.log
set -euo pipefail
LABEL=$1; MODE=$2; CACHE_FLAG=$3; USERS=${4:-50}; SECS=${5:-60}
mkdir -p results/api

# Make sure no API from a previous round is still holding the port.
for i in $(seq 1 30); do curl -s localhost:8000/health >/dev/null || break; sleep 1; done
if curl -s localhost:8000/health >/dev/null; then echo "port 8000 still in use, aborting"; exit 1; fi

DB_MODE=$MODE CACHE=$CACHE_FLAG python -m uvicorn api.app:app --port 8000 --log-level warning \
  > "results/api/${LABEL}_server.log" 2>&1 &
API_PID=$!
trap 'kill $API_PID 2>/dev/null || true; wait $API_PID 2>/dev/null || true' EXIT

for i in $(seq 1 30); do curl -sf localhost:8000/health >/dev/null && break; sleep 1; done
HEALTH=$(curl -s localhost:8000/health); echo "$HEALTH"
# Abort if the API answering is not the one this round started (wrong mode or cache).
WANT_CACHE=$([ "$CACHE_FLAG" = "1" ] && echo true || echo false)
case "$HEALTH" in
  *"\"db_mode\":\"$MODE\""*"\"cache\":$WANT_CACHE"*) ;;
  *) echo "API settings do not match this round, aborting"; exit 1 ;;
esac

python -m locust -f "${LOCUSTFILE:-loadtest/locustfile.py}" --host http://127.0.0.1:8000 --headless \
  -u "$USERS" -r 10 -t "${SECS}s" --csv "results/api/${LABEL}" --only-summary > "results/api/${LABEL}_locust.log" 2>&1 || true
# If the load generator itself saturates a CPU core, QPS is capped by Locust, not the API.
grep -i "cpu usage" "results/api/${LABEL}_locust.log" | head -1 || true

echo "== $LABEL (DB_MODE=$MODE CACHE=$CACHE_FLAG users=$USERS ${SECS}s)"
python - "$LABEL" << 'PY'
import csv, sys
for r in csv.DictReader(open(f"results/api/{sys.argv[1]}_stats.csv")):
    print(f'{r["Name"]:28s} req={r["Request Count"]:>7} fail={r["Failure Count"]:>4} '
          f'qps={float(r["Requests/s"]):8.1f} p50={r["50%"]:>5}ms p95={r["95%"]:>5}ms p99={r["99%"]:>5}ms')
PY
