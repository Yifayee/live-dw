import json
import sys
from pathlib import Path

import anthropic
import duckdb
import pandas as pd

from nl2sql import (DB_PATH, MODEL, build_system_prompt, generate_sql,
                    generate_sql_with_retry, is_read_only)


def sort_key(v):
    """Sort numbers numerically and everything else as text."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return (0, float(v), "")
    return (1, 0.0, str(v))


def col_values(series):
    """Values of one column, floats rounded to 2 decimals, order-insensitive."""
    vals = [round(v, 2) if isinstance(v, float) else v for v in series.tolist()]
    return sorted(vals, key=sort_key)


def results_match(gold_df, pred_df):
    """Relaxed execution match: same row count, and every gold column's values
    appear as some column of the prediction (extra predicted columns allowed)."""
    if len(gold_df) != len(pred_df):
        return False
    pred_cols = [col_values(pred_df[c]) for c in pred_df.columns]
    return all(col_values(gold_df[c]) in pred_cols for c in gold_df.columns)


def main(model, gold_path, use_retry):
    con = duckdb.connect(DB_PATH, read_only=True)
    client = anthropic.Anthropic()
    system_prompt = build_system_prompt(con)

    with open(gold_path) as f:
        gold_set = json.load(f)

    records = []
    for item in gold_set:
        gold_df = con.execute(item["gold_sql"]).df()
        if use_retry:
            pred_sql, retried = generate_sql_with_retry(
                client, system_prompt, item["question"], con, model=model)
        else:
            pred_sql, retried = generate_sql(
                client, system_prompt, item["question"], model=model), False

        status, error = "wrong", ""
        if not is_read_only(pred_sql):
            status = "rejected"
        else:
            try:
                pred_df = con.execute(pred_sql).df()
                if results_match(gold_df, pred_df):
                    status = "correct"
            except Exception as e:
                status, error = "error", str(e)[:200]

        records.append({"id": item["id"], "question": item["question"],
                        "status": status, "retried": retried,
                        "pred_sql": pred_sql, "error": error})
        flag = " (retried)" if retried else ""
        print(f"[{status:>8}]{flag} {item['id']}: {item['question']}")

    results = pd.DataFrame(records)
    n_correct = int((results["status"] == "correct").sum())
    n_retried = int(results["retried"].sum())
    print(f"\nModel: {model} | Set: {gold_path} | Retry: {use_retry}")
    print(f"Execution accuracy: {n_correct}/{len(results)} = {n_correct / len(results):.1%}")
    print(f"Questions that used a retry: {n_retried}")

    tag = Path(gold_path).stem + ("_retry" if use_retry else "")
    results.to_csv(f"eval/results_{model}_{tag}.csv", index=False)


if __name__ == "__main__":
    # Usage: python scripts/evaluate.py [gold_path] [model] [--retry]
    args = sys.argv[1:]
    use_retry = "--retry" in args
    positional = [a for a in args if not a.startswith("--")]
    gold_path = positional[0] if positional else "eval/gold.json"
    model = positional[1] if len(positional) > 1 else MODEL
    main(model, gold_path, use_retry)