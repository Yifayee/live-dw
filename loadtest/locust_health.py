"""
Ceiling check: the same load against GET /health, which touches no database.
If /health tops out near the metric endpoints' QPS, the limit is the web stack
(one uvicorn process, Python request handling), not DuckDB.

  LOCUSTFILE=loadtest/locust_health.py bash loadtest/run.sh r4_health_ceiling shared 0
"""
from locust import HttpUser, between, task


class HealthUser(HttpUser):
    wait_time = between(0, 0.05)

    @task
    def health(self):
        self.client.get("/health")
