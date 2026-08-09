import os
import json
import time
import asyncio
from typing import Dict, Any
import uuid
import random

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from faker import Faker

fake = Faker()

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC_NAME", "financial-transactions")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../../schemas/transaction.avsc")

def load_schema(schema_path: str) -> str:
    with open(schema_path, "r") as f:
        return f.read()

def generate_transaction() -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 10000)}",
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": "USD",
        "merchant_id": f"merchant_{random.randint(1, 500)}",
        "location": fake.city(),
        "device_fingerprint": str(uuid.uuid4())[:8],
        "timestamp_ms": int(time.time() * 1000)
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    # Commenting out success to avoid console spam at high throughput
    # else:
    #     pass

def main():
    schema_str = load_schema(SCHEMA_PATH)
    
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    string_serializer = StringSerializer('utf_8')
    
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'key.serializer': string_serializer,
        'value.serializer': avro_serializer,
    }
    
    producer = SerializingProducer(producer_conf)
    
    print(f"Starting to produce messages to {TOPIC}...")
    
    try:
        count = 0
        start_time = time.time()
        while True:
            txn = generate_transaction()
            # Produce asynchronously
            producer.produce(topic=TOPIC, key=txn['user_id'], value=txn, on_delivery=delivery_report)
            producer.poll(0)
            
            count += 1
            if count % 10000 == 0:
                elapsed = time.time() - start_time
                print(f"Produced {count} messages... Rate: {10000/elapsed:.2f} msg/sec")
                start_time = time.time()
                
            # Sleep tiny amount to control rate on local machine.
            # On M4 16GB, we don't want to crash Docker, so limit to ~5k msg/sec
            time.sleep(0.0002) 
            
    except KeyboardInterrupt:
        print("\nInterrupted. Flushing...")
    finally:
        producer.flush()
        print("Done.")

if __name__ == "__main__":
    main()
