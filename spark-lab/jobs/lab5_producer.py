"""
Lab 5 — Kafka Producer
======================
Generates fake e-commerce events and sends them to Kafka.
Run INSIDE the docker network so it can reach the kafka container.

Usage:
  docker run --rm -it \
    --network spark-lab_spark-net \
    -v $(pwd)/jobs:/jobs \
    python:3.11-slim \
    bash -c "pip install kafka-python-ng && python /jobs/lab5_producer.py"
"""

import json
import time
import random
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "orders"
BOOTSTRAP = "kafka:9092"

PRODUCTS = ["laptop", "phone", "tablet", "headphones", "monitor", "keyboard"]
REGIONS = ["EU", "US", "APAC"]

print(f"Connecting to Kafka at {BOOTSTRAP}...")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
print("Connected. Producing events...\n")

order_id = 2000
try:
    while True:
        product = random.choice(PRODUCTS)
        region = random.choice(REGIONS)
        quantity = random.randint(1, 5)
        amount = round(random.uniform(49.99, 1499.99), 2)

        event = {
            "order_id": order_id,
            "order_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "product": product,
            "region": region,
            "amount": amount,
            "quantity": quantity,
        }

        producer.send(TOPIC, value=event)
        print(f"  → order {order_id}: {product:12s} {region:4s} ${amount:8.2f} x{quantity}")

        order_id += 1
        time.sleep(1)  # 1 event per second

except KeyboardInterrupt:
    print(f"\nStopped. Produced {order_id - 2000} events.")
finally:
    producer.close()
