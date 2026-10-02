import duckdb

con = duckdb.connect("warehouse.duckdb")

# Load raw file as-is; column names are assigned manually (file has no header)
con.execute("""
CREATE OR REPLACE TABLE raw_twitch AS
SELECT * FROM read_csv(
    'data/raw/100k_a.csv',
    header = false,
    columns = {
        'user_id': 'BIGINT',
        'stream_id': 'BIGINT',
        'streamer': 'VARCHAR',
        'time_start': 'INTEGER',
        'time_stop': 'INTEGER'
    }
)
""")

checks = {
    # Scale of the dataset
    "scale": """
        SELECT COUNT(*) AS n_rows,
               COUNT(DISTINCT user_id) AS n_users,
               COUNT(DISTINCT stream_id) AS n_streams,
               COUNT(DISTINCT streamer) AS n_streamers
        FROM raw_twitch""",
    # Time range (units still to be confirmed from the dataset page)
    "time_range": """
        SELECT MIN(time_start), MAX(time_start), MIN(time_stop), MAX(time_stop)
        FROM raw_twitch""",
    # Null counts per column
    "nulls": """
        SELECT COUNT(*) FILTER (WHERE user_id IS NULL) AS null_user,
               COUNT(*) FILTER (WHERE stream_id IS NULL) AS null_stream,
               COUNT(*) FILTER (WHERE streamer IS NULL) AS null_streamer,
               COUNT(*) FILTER (WHERE time_start IS NULL OR time_stop IS NULL) AS null_time
        FROM raw_twitch""",
    # Invalid intervals: stop earlier than start
    "bad_intervals": """
        SELECT COUNT(*) AS n_stop_before_start
        FROM raw_twitch WHERE time_stop < time_start""",
    # Exact duplicate rows
    "duplicates": """
        SELECT COUNT(*) - COUNT(DISTINCT (user_id, stream_id, streamer, time_start, time_stop))
               AS n_exact_duplicates
        FROM raw_twitch""",
    # Distribution of session length in raw time units
    "duration_dist": """
        SELECT MIN(d), APPROX_QUANTILE(d, 0.5) AS p50,
               APPROX_QUANTILE(d, 0.9) AS p90, MAX(d)
        FROM (SELECT time_stop - time_start AS d FROM raw_twitch)""",
    # Does one stream_id map to more than one streamer? (key consistency)
    "stream_key_check": """
        SELECT COUNT(*) AS n_streams_with_multiple_streamers
        FROM (SELECT stream_id FROM raw_twitch
              GROUP BY stream_id HAVING COUNT(DISTINCT streamer) > 1)""",
    # Top 10 streamers by number of interactions
    "top_streamers": """
        SELECT streamer, COUNT(*) AS n_interactions,
               COUNT(DISTINCT user_id) AS n_viewers
        FROM raw_twitch GROUP BY streamer
        ORDER BY n_interactions DESC LIMIT 10""",
            # Can one user have multiple rows for the same stream? (decides DWD grain)
    "grain_check": """
        SELECT COUNT(*) AS n_user_stream_pairs_with_multiple_rows
        FROM (SELECT user_id, stream_id FROM raw_twitch
              GROUP BY user_id, stream_id HAVING COUNT(*) > 1)""",
}

for name, sql in checks.items():
    print(f"\n=== {name} ===")
    print(con.execute(sql).df().to_string(index=False))