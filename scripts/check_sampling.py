import duckdb

con = duckdb.connect("warehouse.duckdb", read_only=True)

# If the 100k subset was filtered by activity, min sessions per user will be > 1
print("\n=== user_activity_floor ===")
print(con.execute("""
    SELECT MIN(n_sessions)                      AS min_sessions,
           APPROX_QUANTILE(n_sessions, 0.1)     AS p10_sessions,
           APPROX_QUANTILE(n_sessions, 0.5)     AS p50_sessions,
           MIN(active_days)                     AS min_active_days,
           COUNT(*) FILTER (WHERE n_sessions = 1) AS n_users_with_one_session
    FROM dim_user
""").df().to_string(index=False))

# Full 43-day series, to judge day-to-day variation in DAU and PCU
print("\n=== daily_series ===")
print(con.execute("""
    SELECT day_index, dau, pcu, avg_watch_min_per_dau, is_full_day
    FROM ads_platform_daily
    ORDER BY day_index
""").df().to_string(index=False))