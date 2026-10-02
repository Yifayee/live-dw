"""
Step 2: Validate grain and key assumptions before modeling.
Run from live-dw root:  python scripts/criteo_check_grain.py
"""
from pathlib import Path

import duckdb

PARQUET = Path("data/raw/criteo/criteo_attribution.parquet")

con = duckdb.connect()
con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet('{PARQUET}')")


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


# ---------- 1. Exact duplicate rows ----------
section("1. EXACT DUPLICATE ROWS")
con.sql("""
    SELECT
        (SELECT COUNT(*) FROM raw)                            AS total_rows,
        (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM raw))   AS distinct_rows
""").show()

# ---------- 2. Candidate key: (timestamp, uid, campaign) ----------
section("2. CANDIDATE KEY (timestamp, uid, campaign)")
con.sql("""
    SELECT
        COUNT(*)  AS keys_with_duplicates,
        SUM(n)    AS rows_involved
    FROM (
        SELECT timestamp, uid, campaign, COUNT(*) AS n
        FROM raw
        GROUP BY ALL
        HAVING COUNT(*) > 1
    )
""").show()

# ---------- 3. Sentinel (-1) patterns by conversion and click ----------
section("3. SENTINEL (-1) PATTERNS BY conversion x click")
con.sql("""
    SELECT
        conversion,
        click,
        COUNT(*)                                   AS n_rows,
        count_if(conversion_id = -1)               AS conv_id_missing,
        count_if(conversion_timestamp = -1)        AS conv_ts_missing,
        count_if(click_pos = -1)                   AS click_pos_missing,
        count_if(click_nb = -1)                    AS click_nb_missing,
        count_if(time_since_last_click = -1)       AS tslc_missing,
        count_if(attribution = 1)                  AS attributed
    FROM raw
    GROUP BY ALL
    ORDER BY ALL
""").show()

# ---------- 4. Consistency within each conversion journey ----------
section("4. CONSISTENCY WITHIN conversion_id")
con.sql("""
    WITH journeys AS (
        SELECT
            conversion_id,
            COUNT(*)                              AS n_rows,
            COUNT(DISTINCT uid)                   AS n_uid,
            COUNT(DISTINCT campaign)              AS n_campaign,
            COUNT(DISTINCT conversion_timestamp)  AS n_conv_ts,
            COUNT(DISTINCT attribution)           AS n_attribution
        FROM raw
        WHERE conversion = 1
        GROUP BY conversion_id
    )
    SELECT
        COUNT(*)                         AS conversions,
        ROUND(AVG(n_rows), 2)            AS avg_impressions_per_conversion,
        MAX(n_rows)                      AS max_impressions_per_conversion,
        count_if(n_uid > 1)              AS multi_uid,
        count_if(n_campaign > 1)         AS multi_campaign,
        count_if(n_conv_ts > 1)          AS multi_conv_ts,
        count_if(n_attribution > 1)      AS mixed_attribution
    FROM journeys
""").show()

# ---------- 5. Logical ordering checks ----------
section("5. LOGICAL ORDERING")
con.sql("""
    SELECT
        count_if(conversion = 1
                 AND conversion_timestamp < timestamp)          AS conv_before_impression,
        count_if(click_pos >= 0 AND click_nb >= 0
                 AND click_pos >= click_nb)                     AS click_pos_out_of_range
    FROM raw
""").show()