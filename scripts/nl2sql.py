import json
import re

import anthropic
import duckdb

DB_PATH = "warehouse.duckdb"
MANIFEST_PATH = "target/manifest.json"
MODEL = "claude-haiku-4-5-20251001"

# Only curated layers are exposed to the LLM; the raw ODS table is excluded
EXPOSED_TABLES = [
    "dwd_watch_session", "dim_user", "dim_streamer", "dim_data_window",
    "dws_user_day", "dws_streamer_day",
    "ads_platform_concurrency", "ads_platform_daily", "ads_user_retention",
]

BUSINESS_RULES = """
- 1 time step = 10 minutes; 144 steps = 1 day. day_index is a relative day (0-42), not a calendar date.
- Day 42 is partial. When comparing or ranking days, use only rows where is_full_day is true,
  unless the question explicitly asks otherwise.
- "Viewers" means users present in a stream's chat, among 100k sampled users
  who each have at least 5 records.
- Prefer ADS and DWS tables for daily or aggregated questions;
  use dwd_watch_session only for session-level detail.
- This is DuckDB: "/" always returns a decimal, even between integers.
  Use "//" for integer division.
"""

SYSTEM_TEMPLATE = """You write DuckDB SQL for a live-streaming data warehouse.
Return exactly one read-only SQL query (SELECT or WITH) and nothing else:
no explanation, no markdown fences.

Schema:
{schema}

Business rules:
{rules}"""


def build_schema_context(con):
    """Combine column names/types from DuckDB with descriptions from dbt's manifest."""
    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)
    docs = {
        node["name"]: node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
    }

    lines = []
    for table in EXPOSED_TABLES:
        node = docs.get(table, {})
        table_desc = " ".join(node.get("description", "").split())
        col_docs = node.get("columns", {})
        lines.append(f"TABLE {table}: {table_desc}")

        cols = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()
        for name, dtype in cols:
            col_desc = col_docs.get(name, {}).get("description", "")
            suffix = f"  -- {col_desc}" if col_desc else ""
            lines.append(f"  {name} {dtype}{suffix}")
        lines.append("")
    return "\n".join(lines)


def build_system_prompt(con):
    return SYSTEM_TEMPLATE.format(schema=build_schema_context(con), rules=BUSINESS_RULES)


def ask_model(client, system_prompt, messages, model=MODEL):
    """Send a conversation to Claude and return the SQL text it replies with."""
    response = client.messages.create(
        model=model,
        max_tokens=1000,
        system=system_prompt,
        messages=messages,
    )
    text = response.content[0].text.strip()
    # Strip markdown fences in case the model adds them anyway
    return re.sub(r"^```(?:sql)?\s*|\s*```$", "", text).strip()


def generate_sql(client, system_prompt, question, model=MODEL):
    """Single attempt: one question in, one SQL query out."""
    return ask_model(client, system_prompt, [{"role": "user", "content": question}], model)


def generate_sql_with_retry(client, system_prompt, question, con, model=MODEL):
    """Generate SQL; if it raises an execution error, send the error back once
    and ask for a corrected query. Returns (final_sql, retried)."""
    messages = [{"role": "user", "content": question}]
    sql = ask_model(client, system_prompt, messages, model)
    if not is_read_only(sql):
        return sql, False
    try:
        con.execute(sql)
        return sql, False
    except Exception as e:
        messages += [
            {"role": "assistant", "content": sql},
            {"role": "user", "content": (
                f"That query failed with this DuckDB error:\n{e}\n"
                "Return a corrected query only."
            )},
        ]
        return ask_model(client, system_prompt, messages, model), True


def is_read_only(sql):
    """Layer 1 safety check: allow a single SELECT or WITH statement only."""
    stripped = sql.strip().rstrip(";").strip()
    if ";" in stripped:
        return False
    return re.match(r"(?is)^(select|with)\b", stripped) is not None


if __name__ == "__main__":
    import sys

    question = " ".join(sys.argv[1:])
    # Layer 2 safety: the database connection itself is read-only
    con = duckdb.connect(DB_PATH, read_only=True)
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from the environment

    sql, retried = generate_sql_with_retry(client, build_system_prompt(con), question, con)
    print("Generated SQL" + (" (after one retry)" if retried else "") + ":\n" + sql + "\n")
    if not is_read_only(sql):
        print("Rejected: not a single read-only query")
    else:
        print(con.execute(sql).df().to_string(index=False))