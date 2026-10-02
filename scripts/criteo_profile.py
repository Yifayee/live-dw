"""
Step 1: Convert raw Criteo TSV to Parquet, then profile the data.
Run from live-dw root:  python scripts/criteo_profile.py
"""
import time
from pathlib import Path

import duckdb

RAW = Path("data/raw/criteo/criteo_attribution_dataset.tsv.gz")
PARQUET = Path("data/raw/criteo/criteo_attribution.parquet")

con = duckdb.connect()

# ---------- 1. One-time conversion: gzipped TSV -> Parquet ----------
if not PARQUET.exists():
    print("Converting TSV.gz to Parquet (one-time, may take a few minutes)...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv('{RAW}', delim='\t', header=true)
        ) TO '{PARQUET}' (FORMAT parquet, COMPRESSION zstd)
    """)
    print(f"Done in {time.time() - t0:.1f} s")

con.execute(f"CREATE VIEW ads AS SELECT * FROM read_parquet('{PARQUET}')")


def section(title):
    print("\n" + "=" * 70 + f"\n{title}\n" + "=" * 70)


# ---------- 2. Schema: column names and types ----------
section("SCHEMA")
con.sql("DESCRIBE ads").show(max_rows=50)

# ---------- 3. Sample rows ----------
section("SAMPLE ROWS")
con.sql("SELECT * FROM ads LIMIT 5").show()

# ---------- 4. Column-level summary (min/max/nulls/distinct) ----------
section("COLUMN SUMMARY")
con.sql("""
    SELECT column_name, column_type, min, max,
           approx_unique, null_percentage
    FROM (SUMMARIZE ads)
""").show(max_rows=50)

# ---------- 5. Overall volume and funnel ----------
section("OVERALL FUNNEL")
con.sql("""
    SELECT
        COUNT(*)                                        AS impressions,
        COUNT(DISTINCT uid)                             AS users,
        COUNT(DISTINCT campaign)                        AS campaigns,
        SUM(click)                                      AS clicks,
        ROUND(SUM(click) / COUNT(*), 4)                 AS ctr,
        SUM(conversion)                                 AS rows_flagged_conversion,
        COUNT(DISTINCT CASE WHEN conversion = 1
                            THEN conversion_id END)     AS distinct_conversions,
        ROUND(SUM(cost), 2)                             AS total_cost
    FROM ads
""").show()

# ---------- 6. Time range and daily volume ----------
section("DAILY VOLUME (timestamp // 86400)")
con.sql("""
    SELECT
        CAST(timestamp // 86400 AS INTEGER)  AS day,
        COUNT(*)                              AS impressions,
        SUM(click)                            AS clicks,
        ROUND(SUM(cost), 2)                   AS cost
    FROM ads
    GROUP BY 1
    ORDER BY 1
""").show(max_rows=40)

# ---------- 7. Campaign concentration ----------
section("TOP 10 CAMPAIGNS BY IMPRESSIONS")
con.sql("""
    SELECT
        campaign,
        COUNT(*)                         AS impressions,
        SUM(click)                       AS clicks,
        ROUND(SUM(click) / COUNT(*), 4)  AS ctr,
        ROUND(SUM(cost), 2)              AS cost
    FROM ads
    GROUP BY 1
    ORDER BY impressions DESC
    LIMIT 10
""").show()