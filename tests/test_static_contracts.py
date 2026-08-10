import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StaticContractTests(unittest.TestCase):
    def test_transaction_schema_contains_risk_geo_fields(self) -> None:
        schema = json.loads((ROOT / "schemas" / "transaction.avsc").read_text())
        fields = {field["name"] for field in schema["fields"]}

        self.assertTrue(
            {
                "transaction_id",
                "user_id",
                "amount",
                "merchant_id",
                "location",
                "latitude",
                "longitude",
                "device_fingerprint",
                "timestamp_ms",
            }.issubset(fields)
        )

    def test_clickhouse_ingests_transactions_and_risk_alerts(self) -> None:
        sql = (ROOT / "docker" / "clickhouse-init" / "01_init.sql").read_text()

        self.assertIn("ENGINE = Kafka", sql)
        self.assertIn("kafka_topic_list = 'financial-transactions'", sql)
        self.assertIn("kafka_topic_list = 'fraud-alerts'", sql)
        self.assertIn("format_avro_schema_registry_url = 'http://redpanda:8087'", sql)
        self.assertIn("geo_spread_km Float64", sql)
        self.assertIn("distinct_devices_60s UInt64", sql)

    def test_api_uses_parameterized_clickhouse_queries_and_auth_hooks(self) -> None:
        api = (ROOT / "apps" / "fast-api-engine" / "main.py").read_text()

        self.assertIn("DISVENT_API_KEY", api)
        self.assertIn("APIKeyHeader", api)
        self.assertIn("parameters={", api)
        self.assertIn("WHERE merchant_id = {merchant_id:String}", api)
        self.assertIn("WHERE user_id = {user_id:String}", api)
        self.assertIn("prometheus_client", api)

    def test_compose_has_full_stack_profiles(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text()

        for service in (
            "api:",
            "dashboard:",
            "generator:",
            "streaming-engine:",
            "prometheus:",
            "grafana:",
        ):
            self.assertIn(service, compose)

        self.assertIn('profiles: ["pipeline"]', compose)
        self.assertIn('profiles: ["observability", "pipeline"]', compose)


if __name__ == "__main__":
    unittest.main()
