import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
from faker import Faker


SCHEMA_PATH_ENV = os.getenv("SCHEMA_PATH")
if SCHEMA_PATH_ENV:
    SCHEMA_PATH = Path(SCHEMA_PATH_ENV)
else:
    SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "transaction.avsc"
MERCHANT_IDS = [f"merchant_{idx}" for idx in range(1, 501)]
USER_IDS = [f"user_{idx}" for idx in range(1, 10_001)]
CITY_COORDS = {
    "New York": (40.7128, -74.0060),
    "London": (51.5074, -0.1278),
    "Singapore": (1.3521, 103.8198),
    "Dubai": (25.2048, 55.2708),
    "San Francisco": (37.7749, -122.4194),
    "Mumbai": (19.0760, 72.8777),
    "Tokyo": (35.6762, 139.6503),
    "Berlin": (52.5200, 13.4050),
}


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")
    topic: str = os.getenv("TOPIC_NAME", "financial-transactions")
    target_rate: int = int(os.getenv("TARGET_RATE_PER_SEC", "2500"))
    flush_every: int = int(os.getenv("FLUSH_EVERY", "5000"))
    max_messages: int = int(os.getenv("MAX_MESSAGES", "0"))
    anomaly_probability: float = float(os.getenv("ANOMALY_PROBABILITY", "0.035"))


class TransactionFactory:
    def __init__(self, anomaly_probability: float) -> None:
        self.fake = Faker()
        self.hot_users = random.sample(USER_IDS, 50)
        self.anomaly_probability = anomaly_probability

    def make(self) -> dict[str, Any]:
        is_anomaly = random.random() < self.anomaly_probability
        
        if is_anomaly:
            scenario = random.choice(["botnet", "structuring", "high_amount"])
            user_id = random.choice(self.hot_users)
            
            if scenario == "botnet":
                # Botnet: Rapid small transactions from wildly different locations & devices
                amount = round(random.uniform(10.0, 50.0), 2)
                location, latitude, longitude = self._location(is_anomaly=True)
                device_fingerprint = uuid.uuid4().hex[:16] # New device every time
            elif scenario == "structuring":
                # Micro-structuring: Amounts just under the $8000 threshold
                amount = round(random.uniform(7950.0, 7999.0), 2)
                location, latitude, longitude = self._location(is_anomaly=False)
                device_fingerprint = f"device_{user_id[-4:]}" # Same device for the user
            else:
                # High amount: Obvious large single transaction
                amount = round(random.uniform(8500.0, 9500.0), 2)
                location, latitude, longitude = self._location(is_anomaly=False)
                device_fingerprint = f"device_{user_id[-4:]}"
        else:
            user_id = random.choice(USER_IDS)
            amount = round(random.lognormvariate(4.0, 1.0), 2)
            location, latitude, longitude = self._location(is_anomaly=False)
            device_fingerprint = uuid.uuid4().hex[:16]

        return {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": amount,
            "currency": "USD",
            "merchant_id": random.choice(MERCHANT_IDS),
            "location": location,
            "latitude": latitude,
            "longitude": longitude,
            "device_fingerprint": device_fingerprint,
            "timestamp_ms": int(time.time() * 1000),
        }

    def _location(self, is_anomaly: bool) -> tuple[str, float, float]:
        if is_anomaly:
            city = random.choice(tuple(CITY_COORDS))
            lat, lon = CITY_COORDS[city]
            return city, lat, lon

        lat = float(self.fake.latitude())
        lon = float(self.fake.longitude())
        return self.fake.city(), lat, lon


def load_schema() -> str:
    return SCHEMA_PATH.read_text()


def delivery_report(err: KafkaException | None, msg: Any) -> None:
    if err is not None:
        print(f"delivery_failed key={msg.key()!r} error={err}", flush=True)


def build_producer(settings: Settings) -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": settings.schema_registry_url})
    avro_serializer = AvroSerializer(schema_registry_client, load_schema())

    return SerializingProducer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "linger.ms": 10,
            "batch.num.messages": 10_000,
            "compression.type": "snappy",
            "queue.buffering.max.messages": 1_000_000,
        }
    )


def main() -> None:
    settings = Settings()
    producer = build_producer(settings)
    factory = TransactionFactory(settings.anomaly_probability)
    running = True

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sleep_interval = 1 / max(settings.target_rate, 1)
    produced = 0
    window_count = 0
    window_started_at = time.monotonic()

    print(
        "generator_started "
        f"topic={settings.topic} kafka={settings.kafka_bootstrap_servers} "
        f"schema_registry={settings.schema_registry_url} target_rate={settings.target_rate}/sec",
        flush=True,
    )

    while running and (settings.max_messages == 0 or produced < settings.max_messages):
        txn = factory.make()
        producer.produce(
            topic=settings.topic,
            key=txn["user_id"],
            value=txn,
            on_delivery=delivery_report,
        )
        producer.poll(0)

        produced += 1
        window_count += 1

        if produced % settings.flush_every == 0:
            producer.flush(5)
            elapsed = max(time.monotonic() - window_started_at, 0.001)
            print(
                f"produced={produced} recent_rate={window_count / elapsed:.0f}/sec",
                flush=True,
            )
            window_count = 0
            window_started_at = time.monotonic()

        time.sleep(sleep_interval)

    producer.flush(10)
    print(f"generator_stopped produced={produced}", flush=True)


if __name__ == "__main__":
    main()
