import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)

checks = {
    # Dimension row counts should equal distinct counts found in Stage 1
    "dim_counts": """
        SELECT (SELECT COUNT(*) FROM dim_user)     AS n_dim_user,      -- expect 100000
               (SELECT COUNT(*) FROM dim_streamer) AS n_dim_streamer   -- expect 162625
    """,
    # Total sessions must be preserved through aggregation
    "session_reconciliation": """
        SELECT (SELECT SUM(n_sessions) FROM dws_user_day)     AS from_user_day,     -- expect 3051733
               (SELECT SUM(n_sessions) FROM dws_streamer_day) AS from_streamer_day  -- expect 3051733
    """,
    # Total watch minutes must match across DWD and both DWS tables
    "watch_min_reconciliation": """
        SELECT (SELECT SUM(duration_min) FROM dwd_watch_session) AS dwd_total,
               (SELECT SUM(watch_min) FROM dws_user_day)         AS user_day_total,
               (SELECT SUM(watch_min) FROM dws_streamer_day)     AS streamer_day_total
    """,
}

for name, sql in checks.items():
    print(f"\n=== {name} ===")
    print(con.execute(sql).df().to_string(index=False))