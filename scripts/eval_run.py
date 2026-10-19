"""
Text-to-SQL evaluation, one round = N runs over a question set, logged for comparison.

Execution accuracy (the headline metric)
  A prediction is correct when, after running both the gold SQL and the predicted SQL
  on warehouse.duckdb:
    relaxed  same number of rows, and every gold column's values (order-insensitive)
             equal the values of some predicted column; numbers are compared with a
             relative tolerance of 1e-4 (scripts/evaluate.py rounded to 2 decimals, which
             accepted AVG(daily ctr) = 0.3619 for an overall CTR of 0.3612).
             A share returned as a percentage (x100) counts as wrong.
             Extra predicted columns are allowed (e.g. returning the day AND its DAU).
    strict   relaxed, and the prediction has exactly as many columns as the gold query.
  Accuracy = correct / questions, per run; reported as mean, min and max over runs.
  The model is called with its default temperature, so runs differ; "stable" questions
  are correct in every run.

Error taxonomy (every non-correct prediction gets exactly one label, first match wins)
  no_sql_returned     answered in prose instead of SQL (e.g. "no table has ad data")
  multi_statement     more than one SQL statement (blocked by the read-only check)
  rejected            any other non-read-only output (blocked before execution)
  schema_not_exposed  uses a table the prompt never showed, or the error says it does not exist
  exec_error          any other execution error (after the one allowed retry)
  missing_flag_filter gold filters on a business flag (is_full_day, is_mature_day,
                      is_rankable) and the prediction does not
  extra_flag_filter   the prediction filters on a business flag the gold query does not use
  wrong_table         reads from a different set of tables than the gold query
  aggregation         different aggregate functions than the gold query (e.g. AVG of a
                      ratio column instead of SUM/SUM)
  row_count           right tables and filters, wrong number of rows
  value_mismatch      anything else that returns different values
  The labels come from comparing the two SQL texts with sqlglot, so they are heuristics;
  the details file keeps both queries for manual review.

Outputs
  eval/runs/<label>_details.csv   one row per (question, run)
  eval/runs/<label>_summary.json  accuracy, stability, error counts, config
  eval/runs_log.csv               one appended row per round

  python scripts/eval_run.py --set eval/test_v2.json --label r1_baseline --runs 3 --retry
  python scripts/eval_run.py --set eval/test_v2.json --label r2_ads_exposed --runs 3 --retry --expose-ads
  python scripts/eval_run.py --rescore r1_baseline     # re-score saved predictions, no API calls
"""
import argparse
import csv
import hashlib
import json
import math
import re
import statistics
import subprocess
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import duckdb
import sqlglot
from sqlglot import exp

import nl2sql

AD_TABLES = ["dws_ad_campaign_day", "dim_ad_data_window", "ads_ad_daily", "ads_ad_campaign_performance"]
FLAGS = ("is_full_day", "is_mature_day", "is_rankable")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--set", default="eval/test_v2.json")
    p.add_argument("--label")
    p.add_argument("--rescore", metavar="LABEL",
                   help="recompute match and categories from eval/runs/LABEL_details.csv (no API calls)")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--retry", action="store_true", help="allow one error-feedback retry")
    p.add_argument("--expose-ads", action="store_true", help="add the ad tables to the prompt schema")
    p.add_argument("--rules-file", help="extra business rules appended to the prompt")
    p.add_argument("--model", default=nl2sql.MODEL)
    return p.parse_args()


# ---------- execution match ----------
REL_TOL = 1e-4


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _key(v):
    return (0, float(v), "") if _is_num(v) else (1, 0.0, str(v))


def _col(series):
    return sorted(series.tolist(), key=_key)


def _same(a, b):
    if _is_num(a) and _is_num(b):
        return math.isclose(float(a), float(b), rel_tol=REL_TOL, abs_tol=1e-9)
    return a == b or (a != a and b != b)  # NaN equals NaN


def _same_col(x, y):
    return len(x) == len(y) and all(_same(a, b) for a, b in zip(x, y))


def match(gold_df, pred_df):
    if len(gold_df) != len(pred_df):
        return False, False
    pred_cols = [_col(pred_df[c]) for c in pred_df.columns]
    relaxed = all(any(_same_col(_col(gold_df[c]), pc) for pc in pred_cols) for c in gold_df.columns)
    return relaxed, relaxed and len(pred_df.columns) == len(gold_df.columns)


# ---------- error taxonomy ----------
def _features(sql):
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return None
    cte_names = {c.alias_or_name for c in tree.find_all(exp.CTE)}
    tables = {t.name for t in tree.find_all(exp.Table) if t.name not in cte_names}
    cols = {c.name for c in tree.find_all(exp.Column)}
    aggs = {type(a).__name__ for a in tree.find_all(exp.AggFunc)}
    return {"tables": tables, "flags": {f for f in FLAGS if f in cols}, "aggs": aggs}


def categorize(status, error, pred_sql, gold_sql, exposed, gold_rows, pred_rows):
    if status == "rejected":
        text = pred_sql.strip()
        if not re.match(r"(?is)^(select|with)\b", text):
            return "no_sql_returned"
        if ";" in text.rstrip(";"):
            return "multi_statement"
        return "rejected"
    pf, gf = _features(pred_sql), _features(gold_sql)
    if (pf and pf["tables"] - set(exposed)) or "does not exist" in (error or ""):
        return "schema_not_exposed"
    if status == "error":
        return "exec_error"
    if pf is None or gf is None:
        return "value_mismatch"
    if gf["flags"] - pf["flags"]:
        return "missing_flag_filter"
    if pf["flags"] - gf["flags"]:
        return "extra_flag_filter"
    if pf["tables"] != gf["tables"]:
        return "wrong_table"
    if pf["aggs"] != gf["aggs"]:
        return "aggregation"
    if gold_rows != pred_rows:
        return "row_count"
    return "value_mismatch"


def git_sha():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
    except Exception:
        return ""


def score(con, q, gold_df, pred_sql, exposed):
    status, error, relaxed, strict, pred_rows = "wrong", "", False, False, None
    if not nl2sql.is_read_only(pred_sql):
        status = "rejected"
    else:
        try:
            pred_df = con.execute(pred_sql).df()
            pred_rows = len(pred_df)
            relaxed, strict = match(gold_df, pred_df)
            status = "correct" if relaxed else "wrong"
        except Exception as e:
            status, error = "error", str(e)[:300]
    category = "" if status == "correct" else categorize(
        status, error, pred_sql, q["gold_sql"], exposed, len(gold_df), pred_rows)
    return status, error, strict, category


def rescore(label):
    """Re-score saved predictions with the current match rules and taxonomy."""
    out = Path("eval/runs")
    summary = json.load(open(out / f"{label}_summary.json"))
    rows = list(csv.DictReader(open(out / f"{label}_details.csv")))
    questions = {q["id"]: q for q in json.load(open(summary["question_set"]))}
    con = duckdb.connect(nl2sql.DB_PATH, read_only=True)
    gold = {i: con.execute(q["gold_sql"]).df() for i, q in questions.items()}
    for r in rows:
        r["status"], r["error"], r["strict"], r["category"] = score(
            con, questions[r["id"]], gold[r["id"]], r["pred_sql"], summary["exposed_tables"])
        r["run"] = int(r["run"])
        r["retried"] = r["retried"] == "True"
    new = summarize(rows, list(questions.values()), summary["runs"])
    summary.update(new)
    summary["rescored_at"] = datetime.now().isoformat(timespec="seconds")
    write_outputs(label, rows, summary, log_mode="replace")


def summarize(rows, questions, runs):
    n = len(questions)
    acc = [sum(r["status"] == "correct" for r in rows if r["run"] == k) / n for k in range(1, runs + 1)]
    acc_s = [sum(bool(r["strict"]) for r in rows if r["run"] == k) / n for k in range(1, runs + 1)]
    per_q = Counter(r["id"] for r in rows if r["status"] == "correct")
    by_module = {}
    for m in sorted({q.get("module", "") for q in questions}):
        ids = [q["id"] for q in questions if q.get("module", "") == m]
        by_module[m] = round(sum(per_q[i] for i in ids) / (len(ids) * runs), 4)
    cats = Counter(r["category"] for r in rows if r["category"])
    return {
        "exec_accuracy_relaxed": {"mean": round(statistics.mean(acc), 4),
                                  "min": round(min(acc), 4), "max": round(max(acc), 4),
                                  "per_run": [round(a, 4) for a in acc]},
        "exec_accuracy_strict_mean": round(statistics.mean(acc_s), 4),
        "accuracy_by_module": by_module,
        "stable_correct_questions": sum(1 for q in questions if per_q[q["id"]] == runs),
        "never_correct_questions": [q["id"] for q in questions if per_q[q["id"]] == 0],
        "error_categories": dict(cats.most_common()),
        "retries_used": sum(bool(r["retried"]) for r in rows),
    }


LOG_FIELDS = ["timestamp", "label", "model", "question_set", "questions", "runs", "retry",
              "expose_ads", "rules_file", "prompt_sha", "git_sha", "acc_mean", "acc_min",
              "acc_max", "acc_strict_mean", "stable_correct", "error_categories"]


def write_outputs(label, rows, summary, log_mode="append"):
    out = Path("eval/runs")
    out.mkdir(parents=True, exist_ok=True)
    with open(out / f"{label}_details.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    json.dump(summary, open(out / f"{label}_summary.json", "w"), indent=2)

    entry = {**{k: summary.get(k) for k in LOG_FIELDS[:11]},
             "acc_mean": summary["exec_accuracy_relaxed"]["mean"],
             "acc_min": summary["exec_accuracy_relaxed"]["min"],
             "acc_max": summary["exec_accuracy_relaxed"]["max"],
             "acc_strict_mean": summary["exec_accuracy_strict_mean"],
             "stable_correct": summary["stable_correct_questions"],
             "error_categories": json.dumps(summary["error_categories"])}
    log = Path("eval/runs_log.csv")
    old = list(csv.DictReader(open(log))) if log.exists() else []
    if log_mode == "replace":
        old = [e if e["label"] != label else {**entry, "timestamp": e["timestamp"]} for e in old]
    else:
        old.append(entry)
    with open(log, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        w.writeheader()
        w.writerows(old)
    print(json.dumps(summary, indent=2))


def main():
    args = parse_args()
    if args.rescore:
        return rescore(args.rescore)
    if not args.label:
        raise SystemExit("--label is required")
    import anthropic

    if args.expose_ads:
        nl2sql.EXPOSED_TABLES = nl2sql.EXPOSED_TABLES + AD_TABLES
    if args.rules_file:
        nl2sql.BUSINESS_RULES = nl2sql.BUSINESS_RULES + "\n" + Path(args.rules_file).read_text()
    exposed = list(nl2sql.EXPOSED_TABLES)

    con = duckdb.connect(nl2sql.DB_PATH, read_only=True)
    client = anthropic.Anthropic()
    system_prompt = nl2sql.build_system_prompt(con)
    prompt_sha = hashlib.sha256(system_prompt.encode()).hexdigest()[:12]

    questions = json.load(open(args.set))
    gold = {q["id"]: con.execute(q["gold_sql"]).df() for q in questions}

    rows = []
    t0 = time.time()
    for run in range(1, args.runs + 1):
        for q in questions:
            if args.retry:
                pred_sql, retried = nl2sql.generate_sql_with_retry(
                    client, system_prompt, q["question"], con, model=args.model)
            else:
                pred_sql, retried = nl2sql.generate_sql(
                    client, system_prompt, q["question"], model=args.model), False
            status, error, strict, category = score(con, q, gold[q["id"]], pred_sql, exposed)
            rows.append({"run": run, "id": q["id"], "module": q.get("module", ""),
                         "question": q["question"], "status": status, "strict": strict,
                         "retried": retried, "category": category, "pred_sql": pred_sql,
                         "gold_sql": q["gold_sql"], "error": error})
            print(f"run {run} [{status:>8}] {category:<20} {q['id']}: {q['question']}")
    elapsed = time.time() - t0

    summary = {
        "label": args.label, "timestamp": datetime.now().isoformat(timespec="seconds"),
        "model": args.model, "question_set": args.set, "questions": len(questions),
        "runs": args.runs, "retry": args.retry, "expose_ads": args.expose_ads,
        "rules_file": args.rules_file, "prompt_sha": prompt_sha, "git_sha": git_sha(),
        "exposed_tables": exposed,
        **summarize(rows, questions, args.runs),
        "seconds": round(elapsed, 1),
    }
    write_outputs(args.label, rows, summary)


if __name__ == "__main__":
    main()
