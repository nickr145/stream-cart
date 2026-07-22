#!/usr/bin/env python3
"""Synthetic e-commerce clickstream event producer.

Publishes realistic sessioned events (search -> page_view -> add_to_cart ->
remove_from_cart -> purchase, with drop-off at each funnel stage) to the
`clickstream-events` Kafka topic, keyed by session_id.
"""
import argparse
import json
import random
import threading
import time
import uuid
from dataclasses import dataclass


from kafka import KafkaProducer
from kafka.serializer import Serializer


class JSONValueSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


class StringKeySerializer(Serializer):
    def serialize(self, topic, headers, data):
        return data.encode("utf-8")


@dataclass
class Product:
    product_id: str
    price: float


def build_catalog(size: int) -> list[Product]:
    return [
        Product(product_id=f"prod_{i}", price=round(random.uniform(5.0, 300.0), 2))
        for i in range(size)
    ]


def build_user_pool(size: int) -> list[str]:
    return [f"user_{i}" for i in range(size)]


def make_event(user_id: str, session_id: str, event_type: str, product: Product) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product.product_id,
        "price": product.price,
        "timestamp": time.time(),
    }


def generate_session_plan() -> list[str]:
    """Decide which event_types this session produces, in order, applying
    drop-off probabilities at each funnel stage (view -> cart -> purchase)."""
    plan: list[str] = []

    if random.random() < 0.3:
        plan.append("search")

    plan.extend(["page_view"] * random.randint(1, 4))

    if random.random() < 0.35:  # view -> cart drop-off
        plan.append("add_to_cart")

        if random.random() < 0.15:  # occasionally the item gets removed
            plan.append("remove_from_cart")

        if random.random() < 0.45:  # cart -> purchase drop-off
            plan.append("purchase")

    return plan


def run_session(producer: KafkaProducer, topic: str, catalog: list[Product],
                 user_id: str, speed: float) -> int:
    session_id = str(uuid.uuid4())
    plan = generate_session_plan()
    product = random.choice(catalog)  # v1: one product of interest per session

    for event_type in plan:
        event = make_event(user_id, session_id, event_type, product)
        producer.send(topic, key=session_id, value=event)
        time.sleep(random.uniform(1.0, 4.0) / speed)

    return len(plan)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulated clickstream producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="clickstream-events")
    parser.add_argument("--catalog-size", type=int, default=50)
    parser.add_argument("--users", type=int, default=200)
    parser.add_argument("--session-rate", type=float, default=6.0,
                         help="new sessions started per minute")
    parser.add_argument("--speed", type=float, default=1.0,
                         help="multiplier on how fast events within a session "
                              "fire (2.0 = twice as fast as real dwell time)")
    parser.add_argument("--duration", type=float, default=None,
                         help="seconds to run for; omit to run until Ctrl+C")
    args = parser.parse_args()

    catalog = build_catalog(args.catalog_size)
    users = build_user_pool(args.users)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=JSONValueSerializer(),
        key_serializer=StringKeySerializer(),
        linger_ms=50,
    )

    session_interval = 60.0 / args.session_rate
    start = time.time()
    session_count = 0

    print(f"Producing to '{args.topic}' at ~{args.session_rate} sessions/min "
          f"(catalog={args.catalog_size}, users={args.users}, speed={args.speed}x). "
          f"Ctrl+C to stop.")

    try:
        while args.duration is None or (time.time() - start) < args.duration:
            user_id = random.choice(users)
            threading.Thread(
                target=run_session,
                args=(producer, args.topic, catalog, user_id, args.speed),
                daemon=True,
            ).start()
            session_count += 1
            time.sleep(session_interval)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()
        producer.close()
        print(f"Started {session_count} sessions.")


if __name__ == "__main__":
    main()
