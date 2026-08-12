import os
import random

from locust import HttpUser, between, task


class DisventAPIUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self) -> None:
        api_key = os.getenv("DISVENT_API_KEY")
        if not api_key:
            raise ValueError("DISVENT_API_KEY must be set in the environment")
        self.client.headers.update({"X-API-Key": api_key})
        self.merchant_ids = [f"merchant_{i}" for i in range(1, 501)]
        self.user_ids = [f"user_{i}" for i in range(1, 10001)]

    @task(3)
    def get_merchant_stats(self) -> None:
        merchant_id = random.choice(self.merchant_ids)
        self.client.get(f"/api/v1/merchant/{merchant_id}/stats", name="/api/v1/merchant/[id]/stats")

    @task(3)
    def get_risk_score(self) -> None:
        user_id = random.choice(self.user_ids)
        self.client.get(f"/api/v1/risk-score/{user_id}", name="/api/v1/risk-score/[id]")

    @task(1)
    def get_historical_user(self) -> None:
        user_id = random.choice(self.user_ids)
        self.client.get(f"/api/v1/historical/user/{user_id}", name="/api/v1/historical/user/[id]")

    @task(5)
    def get_realtime_throughput(self) -> None:
        self.client.get("/api/v1/metrics/realtime-throughput", name="/api/v1/metrics/realtime-throughput")
