import json
import sys

import duckdb

# Usage: python scripts/check_gold.py [gold_path]
gold_path = sys.argv[1] if len(sys.argv) > 1 else "eval/gold.json"

con = duckdb.connect("warehouse.duckdb", read_only=True)
with open(gold_path) as f:
    gold_set = json.load(f)

# Print each gold answer so it can be checked by eye before evaluation
for item in gold_set:
    df = con.execute(item["gold_sql"]).df()
    print(f"\n{item['id']}: {item['question']}")
    print(df.to_string(index=False))