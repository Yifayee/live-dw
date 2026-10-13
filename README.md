# live-dw: Live-Streaming and Ad Monetization Data Warehouse

A layered data warehouse built with dbt and DuckDB on two public datasets: Twitch viewing records and Criteo display advertising logs. The project models raw event data into DWD, DIM, DWS and ADS layers, enforces data quality with automated tests, and adds an LLM text-to-SQL interface over the metrics layer.

The full project (15 models, 44 data tests) rebuilds and tests end to end with a single `dbt build` in about 11 seconds on a laptop.

## Architecture

```
raw files (CSV / Parquet)
        |
   sources.yml          ODS: raw data registered as dbt sources
        |
   models/dwd/          cleaned detail facts, one row per event
        |
   models/dim/          entities and shared data-window rules
        |
   models/dws/          daily aggregates, additive measures only
        |
   models/ads/          metrics and rankings for analysis and text-to-SQL
```

Design rules applied across both modules:

- **Grain first.** Every model states its grain, and key assumptions are verified with SQL before modeling.
- **Additive measures in DWS, ratios in ADS.** Rates such as CTR and CVR are always computed from summed numerators and denominators, never averaged.
- **Window rules defined once.** Partial-day and data-maturity rules live in single-row `dim_*_data_window` tables that downstream models reference.
- **Flag, don't drop.** Suspect records are kept and labeled so downstream users can decide how to filter.

## Datasets

| Module | Dataset | Scope |
| --- | --- | --- |
| Live streaming | Twitch chat-presence records, 100k-user subset (Rappaz, McAuley and Aberer, RecSys 2021) | 6,148 ten-minute steps, about 43 days |
| Ad monetization | Criteo Attribution Modeling for Bidding dataset (Diemert, Meynet, Lefortier and Galland, AdKDD 2017) | 16,468,027 impressions, 6,142,256 users, 675 campaigns, 30-day sample |

Both datasets use relative time rather than calendar dates. The Criteo `cost` field is a transformed value released by Criteo, not the real price, so cost-based metrics are only meaningful for relative comparison. The Criteo dataset is licensed for non-commercial use (CC-BY-NC-SA 4.0).

Note on conversion counts: Criteo's dataset page reports about 45K conversions, but this figure could not be reproduced under the counting definitions examined in this project. The downloaded file contains 435,810 distinct valid conversion IDs and 438,730 distinct (conversion ID, user ID) pairs. Among the valid conversion IDs, 236,331 appear with attribution = 1, and 327,860 distinct users are associated with at least one conversion. The reason for the discrepancy remains unresolved. All project metrics are computed from the downloaded file using the processing and validation steps documented below.

## Ad Monetization Module

### Models

| Layer | Model | Grain |
| --- | --- | --- |
| DWD | `dwd_ad_impression` | one row per impression |
| DWD | `dwd_ad_conversion` | one row per conversion, keyed by (conversion_id, user_id) |
| DIM | `dim_ad_data_window` | single row: last full day and last mature day |
| DWS | `dws_ad_campaign_day` | one row per (campaign, day) |
| ADS | `ads_ad_daily` | one row per day: CTR, CVR, CPA, CPM, attribution rate |
| ADS | `ads_ad_campaign_performance` | one row per campaign, with CPA ranking and share of cost |

Conversions are credited to the day of the last click on the journey (click-date attribution), so clicks and conversions on the same row come from the same traffic.

### Findings from data validation

**1. The impression table has no ID, but a natural key exists.**
The source has no impression ID. Validation showed that (timestamp, uid, campaign) is unique across all 16.5 million rows and there are no duplicate rows, so `impression_id` is an md5 hash of those three columns.

**2. Missing values are encoded as -1, not NULL.**
Column profiling reported 0% nulls, but five columns use -1 as a sentinel. These are converted to NULL in DWD so that averages and counts are not distorted. Validation also showed that only clicked impressions are flagged as part of a conversion journey, and click position fields exist only on those rows.

**3. `conversion_id` is not unique: source ID collision.**
A single `conversion_id` can appear on several rows of a journey, so conversions must be deduplicated. Consistency checks then found 2,910 conversion IDs whose rows disagreed on user and conversion time. Every cross-campaign and mixed-attribution anomaly came from this same set of IDs. Regrouping by (conversion_id, user_id) removed all of these inconsistencies, which indicates the IDs were reused across different users' conversions rather than describing one journey.

- Conversion grain corrected from `conversion_id` to (conversion_id, user_id)
- Conversions: 438,730 under the corrected grain vs 435,810 distinct IDs, so a single-key model would undercount by 2,920
- 5,830 conversions are affected and flagged with `is_shared_conversion_id`
- Under the corrected grain, every conversion journey belongs to exactly one campaign

**4. Conversion metrics are biased near the end of the observation window.**
Daily CVR was stable through most of the window, then rose steadily over the last few days. Two hypotheses were tested:

- *Campaign mix shift:* rejected as the main cause. Among 667 campaigns with data in both periods, 506 saw their own CVR rise, with a median increase of 28.1%.
- *Right truncation:* supported. When a journey continues after the window ends, its later clicks are not observed and the conversion is credited to its last in-window click. The average gap from the last observed click to conversion grows from 4.01 days for day-0 clicks to 7.37 days for day-30 clicks, which is consistent with this effect.

A related left-edge effect also appears: journeys near the start are missing clicks before day 0, so average clicks per conversion grows from 1.15 to 2.85 across the window. This affects journey-length metrics but not CVR.

The fix is a configurable data-maturity window (`ad_maturity_days`, default 7). With it, the 7 immature days show a CVR 13.8% higher than the 23 mature days. `ads_ad_daily` keeps all days with an `is_mature_day` flag, and `ads_ad_campaign_performance` ranks campaigns on mature days only. The top 3 campaigns by CPA were unchanged after this correction, so the bias mainly affects day-over-day trends rather than cross-campaign rankings.

### Small-sample handling

Campaigns need at least 30 conversions (`ad_min_conversions_for_rank`) to receive a CPA rank, because CPA from a handful of conversions is too noisy to compare. Both thresholds can be changed without editing SQL:

```bash
dbt build --vars '{ad_maturity_days: 5, ad_min_conversions_for_rank: 50}'
```

## Live-Streaming Module

| Layer | Model | Grain |
| --- | --- | --- |
| DWD | `dwd_watch_session` | one row per (user_id, stream_id) |
| DIM | `dim_user`, `dim_streamer` | one row per user / streamer |
| DIM | `dim_data_window` | single row: last step and last full day |
| DWS | `dws_user_day`, `dws_streamer_day` | one row per (entity, day) |
| ADS | `ads_platform_daily` | DAU, active streamers, PCU, sessions, watch time |
| ADS | `ads_platform_concurrency` | concurrent users per 10-minute step |
| ADS | `ads_user_retention` | D1 and D7 retention by cohort |

Notes on interpretation:

- A viewer is a user present in a stream's chat, crawled every 10 minutes, not every person watching.
- Day 42 is partial, so daily metrics carry an `is_full_day` flag.
- Users first seen on day 0 are left-censored and excluded from retention cohorts.
- Every sampled user has at least 5 records, which suggests an activity filter in sampling, so later retention cohorts are biased upward.

## Text-to-SQL

`scripts/nl2sql.py` translates natural-language questions into SQL against the warehouse using the Anthropic Claude API, and `scripts/evaluate.py` scores it against a gold question set (`eval/gold.json`) and a holdout set (`eval/holdout.json`).

```bash
python scripts/nl2sql.py "How many streams did shroud have in total?"
```

Evaluation with Claude Haiku 4.5 (`claude-haiku-4-5-20251001`), each question scored correct or wrong by `scripts/evaluate.py`:

| Question set | Questions | Correct |
| --- | --- | --- |
| Gold (used during development) | 20 | 20 / 20 in each of 3 runs |
| Holdout (written separately) | 8 | 7 / 8 |

The one holdout miss asked how many streams a streamer had, and the model returned total watch minutes instead of the stream count. The column descriptions in `schema.yml` were then revised to separate `n_streams` (broadcast count) from `total_watch_min` (watch time). Because that fix was informed by a holdout question, the 7 / 8 figure is reported from before the change, and a fresh holdout set would be needed for an unbiased re-test.

## Data Quality

44 dbt data tests run on every build:

- **Uniqueness and not-null** on every model's key, including composite keys checked with custom tests
- **Referential integrity** between DWS tables and dimensions
- **Reconciliation** tests that confirm aggregated totals (impressions, clicks, cost, conversions, watch minutes) match the detail layer exactly
- **Range checks** that CTR, CVR and attribution rate stay within [0, 1]

Exploratory validation scripts are kept in `scripts/` (`criteo_profile.py`, `criteo_check_grain.py`, and the `check_*.py` scripts) so each modeling assumption can be traced back to the query that tested it.

## How to Run

Requirements: Python 3 and the packages in `requirements.txt`.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Criteo data**

```bash
mkdir -p data/raw/criteo
curl -L -o data/raw/criteo/criteo_attribution_dataset.tsv.gz \
  https://huggingface.co/datasets/criteo/criteo-attribution-dataset/resolve/main/criteo_attribution_dataset.tsv.gz
python scripts/criteo_profile.py   # converts to Parquet and prints a profile
```

**Twitch data**

Download `100k_a.csv` from the Twitch dataset folder linked on the [UCSD McAuley Lab dataset page](https://cseweb.ucsd.edu/~jmcauley/datasets.html) ([direct folder](https://mcauleylab.ucsd.edu/public_datasets/gdrive/twitch/)).

Place the file at `data/raw/100k_a.csv`, then load it into `warehouse.duckdb` as the `raw_twitch` table (3,051,733 rows) and print a profile:

```bash
mkdir -p data/raw
mv 100k_a.csv data/raw/
python scripts/explore.py
```

**Build and test**

```bash
dbt build
```

For the text-to-SQL script, set `ANTHROPIC_API_KEY` in your environment or in a `.env` file (excluded from Git).

## Limitations

- Criteo cost is transformed, and the dataset's CTR is far above typical display advertising, likely due to how the sample was built. Absolute values are not representative; comparisons across campaigns and days are the focus.
- The 7-day maturity window is a heuristic based on where daily CVR departs from its stable range, not a derived optimum.
- The ID-collision and truncation explanations are supported by the data but cannot be confirmed against Criteo's internal pipeline.

## Roadmap

- **Streaming:** replay Criteo events through Kafka and compute minute-level CTR and spend with Flink SQL, reconciled against the batch tables
- **Orchestration:** schedule the dbt build and data checks with Airflow
- **Cost:** convert `dwd_ad_impression`, the largest model, to an incremental model

## Project Structure

```
live-dw/
├── models/
│   ├── sources.yml
│   ├── schema.yml
│   ├── dwd/
│   ├── dim/
│   ├── dws/
│   └── ads/
├── tests/          custom data tests (reconciliation, composite keys, ranges)
├── scripts/        profiling, validation, text-to-SQL and evaluation
├── eval/           text-to-SQL question sets and results
├── dbt_project.yml
├── profiles.yml
└── requirements.txt
```
