import os
import random
import time
from datetime import datetime

from locust import HttpUser, between, events, task


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
        with self.client.get("/api/v1/metrics/realtime-throughput", name="/api/v1/metrics/realtime-throughput", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                latest_event = data.get("latest_event_time")
                if latest_event:
                    try:
                        # ClickHouse returns 'YYYY-MM-DD HH:MM:SS'
                        event_dt = datetime.strptime(latest_event, "%Y-%m-%d %H:%M:%S")
                        # We assume the generator and locust run on the same clock (UTC/Local)
                        # We'll calculate a simple diff in milliseconds
                        diff_ms = (datetime.now() - event_dt).total_seconds() * 1000
                        events.request.fire(
                            request_type="E2E_LATENCY",
                            name="latest_event_latency",
                            response_time=diff_ms,
                            response_length=0,
                            exception=None,
                        )
                    except Exception as e:
                        pass
