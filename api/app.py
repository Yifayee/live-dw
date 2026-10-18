"""
Metrics API over the live-dw warehouse (FastAPI + DuckDB).

Endpoints
  GET  /health
  GET  /metrics/live/concurrency?start_step=&end_step=   10-min concurrent viewers (ADS)
  GET  /metrics/live/daily?day_index=                     DAU, PCU, watch time per day
  GET  /metrics/ads/daily?day_index=                      CTR, CVR, CPA, CPM per day
  GET  /metrics/ads/campaign/{campaign_id}                one campaign's performance and CPA rank
  POST /nl2sql  {"question": "..."}                       text-to-SQL (Claude) + execution

Two switches, read from environment variables, so the same code can be load-tested
before and after an optimization:
  DB_MODE=per_request   open a new read-only DuckDB connection for every request (baseline)
  DB_MODE=shared        open one connection at startup; each request gets a cheap cursor
  CACHE=1               cache metric query results in memory (LRU), keyed by SQL + params

Run from the live-dw root:
  DB_MODE=per_request python -m uvicorn api.app:app --port 8000
"""
import os
import sys
import time
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import nl2sql  # noqa: E402  (reuses prompt building, retry and read-only checks)

DB_PATH = os.environ.get("DB_PATH", "warehouse.duckdb")
DB_MODE = os.environ.get("DB_MODE", "shared")
CACHE = os.environ.get("CACHE", "0") == "1"
if DB_MODE not in ("per_request", "shared"):
    raise ValueError("DB_MODE must be per_request or shared")

app = FastAPI(title="live-dw metrics API")
_shared = duckdb.connect(DB_PATH, read_only=True) if DB_MODE == "shared" else None


@contextmanager
def connection():
    if DB_MODE == "shared":
        cur = _shared.cursor()  # DuckDB connections are not thread-safe; cursors are cheap
        try:
            yield cur
        finally:
            cur.close()
    else:
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            yield con
        finally:
            con.close()


def _run(sql, params):
    with connection() as con:
        cur = con.execute(sql, list(params))
        cols = [d[0] for d in cur.description]
        return tuple(tuple(r) for r in cur.fetchall()), tuple(cols)


_run_cached = lru_cache(maxsize=4096)(_run)


def query(sql, params=()):
    rows, cols = (_run_cached if CACHE else _run)(sql, tuple(params))
    return [dict(zip(cols, r)) for r in rows]


@app.get("/health")
def health():
    return {"status": "ok", "db_mode": DB_MODE, "cache": CACHE}


@app.get("/metrics/live/concurrency")
def live_concurrency(start_step: int = Query(..., ge=0), end_step: int = Query(..., ge=0)):
    if end_step < start_step or end_step - start_step > 1440:
        raise HTTPException(400, "need start_step <= end_step and a range of at most 1440 steps")
    rows = query(
        "select step, day_index, concurrent_users from ads_platform_concurrency "
        "where step between ? and ? order by step", (start_step, end_step))
    return {"rows": rows}


@app.get("/metrics/live/daily")
def live_daily(day_index: int | None = None):
    if day_index is None:
        return {"rows": query("select * from ads_platform_daily order by day_index")}
    rows = query("select * from ads_platform_daily where day_index = ?", (day_index,))
    if not rows:
        raise HTTPException(404, "day not found")
    return {"rows": rows}


@app.get("/metrics/ads/daily")
def ads_daily(day_index: int | None = None):
    if day_index is None:
        return {"rows": query("select * from ads_ad_daily order by day_index")}
    rows = query("select * from ads_ad_daily where day_index = ?", (day_index,))
    if not rows:
        raise HTTPException(404, "day not found")
    return {"rows": rows}


@app.get("/metrics/ads/campaign/{campaign_id}")
def ads_campaign(campaign_id: int):
    rows = query("select * from ads_ad_campaign_performance where campaign_id = ?", (campaign_id,))
    if not rows:
        raise HTTPException(404, "campaign not found")
    return {"rows": rows}


class Question(BaseModel):
    question: str


_llm = {}


def _llm_state():
    # Built lazily so metric endpoints work without an API key.
    if not _llm:
        import anthropic
        from dotenv import load_dotenv
        load_dotenv()
        with connection() as con:
            _llm["prompt"] = nl2sql.build_system_prompt(con)
        _llm["client"] = anthropic.Anthropic()
    return _llm


@app.post("/nl2sql")
def text_to_sql(q: Question):
    state = _llm_state()
    t0 = time.perf_counter()
    with connection() as con:
        sql, retried = nl2sql.generate_sql_with_retry(state["client"], state["prompt"], q.question, con)
        t1 = time.perf_counter()
        if not nl2sql.is_read_only(sql):
            raise HTTPException(400, {"error": "generated SQL is not a single read-only query", "sql": sql})
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchmany(200)]
        except Exception as e:
            raise HTTPException(422, {"error": str(e), "sql": sql})
    t2 = time.perf_counter()
    return {"sql": sql, "retried": retried, "rows": rows,
            "timing_ms": {"llm": round((t1 - t0) * 1000, 1), "execute": round((t2 - t1) * 1000, 1)}}
