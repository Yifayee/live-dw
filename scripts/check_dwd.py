import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)

# Row count should match raw_twitch (3,051,733) since no rows are filtered
print(con.execute("SELECT COUNT(*) FROM dwd_watch_session").fetchone())

# Sanity check on derived fields
print(con.execute("""
    SELECT MIN(duration_min), MAX(duration_min),
           MIN(day_index), MAX(day_index)
    FROM dwd_watch_session
""").df().to_string(index=False))
