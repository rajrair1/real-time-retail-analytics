import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def make_event() -> dict:
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(2.5, 250), 2)
    discount = random.choice([0, 0, 0, 0.05, 0.1])
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"C{random.randint(1, 10000):05d}",
        "product_id": f"P{random.randint(1, 500):04d}",
        "store_id": f"S{random.randint(1, 50):03d}",
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_rate": discount,
        "payment_method": random.choice(["card", "cash", "wallet"]),
    }


def main(events: int, rate: int) -> None:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    interval = 1 / max(rate, 1)
    for _ in range(events):
        producer.send(os.getenv("KAFKA_TOPIC", "retail_transactions"), make_event())
        time.sleep(interval)
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--rate", type=int, default=100)
    args = parser.parse_args()
    main(args.events, args.rate)
