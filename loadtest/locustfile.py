"""
Load test for the metric endpoints (not /nl2sql: its latency is the LLM call, and
load-testing it mostly measures the API provider and costs money).

Each simulated user picks an endpoint with fixed weights and random parameters drawn
from the real key ranges, so most requests are distinct queries.

  locust -f loadtest/locustfile.py --host http://127.0.0.1:8000 --headless \
         -u 50 -r 10 -t 60s --csv results/api/<label>
"""
import random

import duckdb
from locust import HttpUser, between, task

MAX_STEP = 6148          # ads_platform_concurrency: steps 0..6148
LIVE_DAYS = 43           # ads_platform_daily: day 0..42
AD_DAYS = 31             # ads_ad_daily: day 0..30
# Real campaign ids, read once (read-only) so requests hit existing rows.
with duckdb.connect("warehouse.duckdb", read_only=True) as _con:
    CAMPAIGNS = [r[0] for r in _con.execute(
        "select campaign_id from ads_ad_campaign_performance").fetchall()]


class MetricsUser(HttpUser):
    wait_time = between(0, 0.05)

    @task(4)
    def concurrency_window(self):
        start = random.randint(0, MAX_STEP - 144)
        self.client.get(f"/metrics/live/concurrency?start_step={start}&end_step={start + 143}",
                        name="/metrics/live/concurrency")

    @task(2)
    def live_day(self):
        self.client.get(f"/metrics/live/daily?day_index={random.randrange(LIVE_DAYS)}",
                        name="/metrics/live/daily")

    @task(2)
    def ads_day(self):
        self.client.get(f"/metrics/ads/daily?day_index={random.randrange(AD_DAYS)}",
                        name="/metrics/ads/daily")

    @task(2)
    def campaign(self):
        self.client.get(f"/metrics/ads/campaign/{random.choice(CAMPAIGNS)}",
                        name="/metrics/ads/campaign")
