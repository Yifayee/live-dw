import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)

checks = {
    # Concurrency can never be negative, and must return to 0 after the last stop
    "concurrency_sanity": """
        SELECT MIN(concurrent_users) AS min_online,                     -- expect >= 0
               MAX(concurrent_users) AS peak_online,
               (SELECT concurrent_users FROM ads_platform_concurrency
                ORDER BY step DESC LIMIT 1) AS online_after_last_step   -- expect 0
        FROM ads_platform_concurrency
    """,
    # 43 days in total, only day 42 flagged as partial
    "daily_shape": """
        SELECT COUNT(*) AS n_days,                                       -- expect 43
               COUNT(*) FILTER (WHERE NOT is_full_day) AS n_partial,     -- expect 1
               MAX(day_index) FILTER (WHERE NOT is_full_day) AS partial_day  -- expect 42
        FROM ads_platform_daily
    """,
    # Sum of DAU over all days must equal the number of rows in dws_user_day
    "dau_reconciliation": """
        SELECT (SELECT SUM(dau) FROM ads_platform_daily) AS sum_dau,
               (SELECT COUNT(*) FROM dws_user_day)       AS n_user_day_rows
    """,
    "daily_first_and_last": """
        (SELECT * FROM ads_platform_daily ORDER BY day_index LIMIT 3)
        UNION ALL
        (SELECT * FROM ads_platform_daily ORDER BY day_index DESC LIMIT 3)
        ORDER BY day_index
    """,
    "retention_sample": """
        (SELECT * FROM ads_user_retention ORDER BY cohort_day LIMIT 5)
        UNION ALL
        (SELECT * FROM ads_user_retention ORDER BY cohort_day DESC LIMIT 8)
        ORDER BY cohort_day
    """,
}

for name, sql in checks.items():
    print(f"\n=== {name} ===")
    print(con.execute(sql).df().to_string(index=False))