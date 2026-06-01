"""
NexusFlow — Kafka Producer (Streaming Simulation)
===================================================
Produces real-time e-commerce events to MSK topics.
Supports both burst mode (replay historical) and live mode.

Topics:
  - orders           → JSON, 6 partitions
  - clickstream      → Avro, 12 partitions
  - inventory-events → JSON, 4 partitions
  - product-reviews  → JSON, 4 partitions
"""

import json
import time
import signal
import logging
import os
import sys
import random
import uuid
from datetime import datetime, timezone
from dataclasses import asdict
from typing import Optional
from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from ecommerce_generator import (
    EcommerceDataGenerator, Order, ClickstreamEvent, Customer
)

# ── LOGGING ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/tmp/kafka_producer.log"),
    ]
)
logger = logging.getLogger("nexusflow.producer")

# ── CLICKSTREAM AVRO SCHEMA ───────────────────────────────
CLICKSTREAM_AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "ClickstreamEvent",
    "namespace": "io.nexusflow.events",
    "fields": [
        {"name": "event_id",          "type": "string"},
        {"name": "session_id",         "type": "string"},
        {"name": "customer_id",        "type": ["null", "string"], "default": None},
        {"name": "event_type",         "type": "string"},
        {"name": "event_ts",           "type": "string"},
        {"name": "page_url",           "type": "string"},
        {"name": "referrer_url",       "type": ["null", "string"], "default": None},
        {"name": "product_sku",        "type": ["null", "string"], "default": None},
        {"name": "product_category",   "type": ["null", "string"], "default": None},
        {"name": "search_query",       "type": ["null", "string"], "default": None},
        {"name": "device_type",        "type": "string"},
        {"name": "browser",            "type": "string"},
        {"name": "os",                 "type": "string"},
        {"name": "ip_hash",            "type": "string"},
        {"name": "user_agent",         "type": "string"},
        {"name": "viewport_width",     "type": "int"},
        {"name": "viewport_height",    "type": "int"},
        {"name": "scroll_depth_pct",   "type": ["null", "int"], "default": None},
        {"name": "time_on_page_sec",   "type": ["null", "int"], "default": None},
        {"name": "utm_source",         "type": ["null", "string"], "default": None},
        {"name": "utm_medium",         "type": ["null", "string"], "default": None},
        {"name": "_ingestion_ts",      "type": "string"},
        {"name": "_partition_key",     "type": "string"},
    ]
})


class NexusFlowProducer:
    """Kafka producer for NexusFlow streaming events."""

    def __init__(self,
                 bootstrap_servers: str,
                 schema_registry_url: Optional[str] = None,
                 num_customers: int = 10_000):

        self.bootstrap_servers = bootstrap_servers
        self._running = True
        self._stats = {t: {"produced": 0, "errors": 0}
                       for t in ["orders", "clickstream", "inventory-events", "product-reviews"]}

        # Producer config
        producer_conf = {
            "bootstrap.servers":            bootstrap_servers,
            "linger.ms":                    5,                  # Small delay to allow batching
            "batch.size":                   65536,          # 64KB batch size for better throughput
            "compression.type":             "lz4",          # Fast compression for lower bandwidth usage
            "acks":                         "all",          # Wait for all replicas to acknowledge
            "retries":                      5,              # Retry up to 5 times on failure
            "retry.backoff.ms":             200,            # Idempotent producer to avoid duplicates on retries
            "enable.idempotence":           True,           # Ensure no duplicates on retries
            "statistics.interval.ms":       60000,          # Log stats every 60s
            "client.id":                    "nexusflow-producer",
            # IAM auth for MSK
            "security.protocol":            "SASL_SSL",     # SASL_SSL for secure authentication
            "sasl.mechanism":               "AWS_MSK_IAM",  # AWS IAM for authentication
            "sasl.jaas.config":             (   
                "software.amazon.msk.auth.iam.IAMLoginModule required;"
            ),
            "sasl.client.callback.handler.class": (
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
            ),
        }

        self.producer = Producer(producer_conf)
        self.string_serializer = StringSerializer("utf_8")

        # Avro serializer for clickstream
        if schema_registry_url:
            sr_conf = {"url": schema_registry_url} 
            schema_registry_client = SchemaRegistryClient(sr_conf)
            self.avro_serializer = AvroSerializer(
                schema_registry_client,
                CLICKSTREAM_AVRO_SCHEMA,
            )
        else:
            self.avro_serializer = None

        # Data generator
        self.gen = EcommerceDataGenerator(num_customers=num_customers)
        logger.info("✅ NexusFlowProducer initialized")

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._running = False

    def _delivery_report(self, topic: str):
        """Return a delivery callback bound to topic stats."""
        def callback(err, msg):
            if err:
                logger.error(f"[{topic}] Delivery failed: {err}")
                self._stats[topic]["errors"] += 1
            else:
                self._stats[topic]["produced"] += 1
        return callback

    def produce_order(self, customer_id: str) -> None:
        """Produce a single order event."""
        order = Order.generate(customer_id)
        payload = json.dumps(asdict(order)).encode("utf-8")

        self.producer.produce(
            topic="orders",
            key=self.string_serializer(order.customer_id),
            value=payload,
            headers={
                "source":      b"order-service",
                "version":     b"2.0",
                "content-type": b"application/json",
            },
            on_delivery=self._delivery_report("orders"),
        )

    def produce_clickstream_event(self,
                                   session_id: str,
                                   customer_id: Optional[str] = None) -> None:
        """Produce a clickstream event (Avro if schema registry available)."""
        event = ClickstreamEvent.generate(session_id, customer_id)
        event_dict = asdict(event)

        if self.avro_serializer:
            value = self.avro_serializer(
                event_dict,
                SerializationContext("clickstream", MessageField.VALUE)
            )
        else:
            value = json.dumps(event_dict).encode("utf-8")

        self.producer.produce(
            topic="clickstream",
            key=self.string_serializer(session_id),
            value=value,
            on_delivery=self._delivery_report("clickstream"),
        )

    def produce_inventory_event(self) -> None:
        """Produce an inventory update event."""
        from ecommerce_generator import PRODUCT_CATALOG, WAREHOUSES
        product = random.choice(PRODUCT_CATALOG)
        warehouse = random.choice(WAREHOUSES)

        event = {
            "event_id":        str(uuid.uuid4()),
            "event_type":      random.choice(["restock", "sale", "adjustment", "return"]),
            "event_ts":        datetime.now(timezone.utc).isoformat(),
            "warehouse_id":    warehouse["id"],
            "product_sku":     product["sku"],
            "quantity_change": random.randint(-50, 200),
            "new_quantity":    random.randint(0, 5000),
            "triggered_by":    random.choice(["order", "manual", "supplier", "return"]),
            "_ingestion_ts":   datetime.now(timezone.utc).isoformat(),
        }

        self.producer.produce(
            topic="inventory-events",
            key=self.string_serializer(product["sku"]),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=self._delivery_report("inventory-events"),
        )

    def run_live_stream(self,
                        orders_per_second: float = 5.0,
                        clicks_per_second: float = 50.0) -> None:
        """
        Run continuous real-time streaming.
        Simulates realistic e-commerce traffic patterns.
        """
        logger.info(f"🚀 Starting live stream: {orders_per_second} orders/s, "
                    f"{clicks_per_second} clicks/s")

        order_interval = 1.0 / orders_per_second
        click_interval = 1.0 / clicks_per_second
        last_order_ts = time.time()
        last_click_ts = time.time()
        last_stats_ts = time.time()
        active_sessions = {}  # session_id → customer_id

        while self._running:
            now = time.time()

            # Produce orders
            if now - last_order_ts >= order_interval:
                customer = random.choice(self.gen.customers)
                self.produce_order(customer.customer_id)
                last_order_ts = now

            # Produce clickstream
            if now - last_click_ts >= click_interval:
                # Manage sessions
                if len(active_sessions) < 1000 or random.random() < 0.05:
                    session_id = str(uuid.uuid4())
                    customer = random.choice(self.gen.customers) if random.random() > 0.3 else None
                    active_sessions[session_id] = customer.customer_id if customer else None

                session_id = random.choice(list(active_sessions.keys()))
                customer_id = active_sessions[session_id]
                self.produce_clickstream_event(session_id, customer_id)
                last_click_ts = now

            # Inventory events (occasional)
            if random.random() < 0.01:
                self.produce_inventory_event()

            # Flush periodically
            self.producer.poll(0)

            # Stats every 60s
            if now - last_stats_ts >= 60:
                self._log_stats()
                last_stats_ts = now

            time.sleep(0.001)  # Prevent tight loop

        logger.info("Flushing remaining messages...")
        self.producer.flush(timeout=30)
        self._log_stats()

    def run_burst_replay(self, num_orders: int = 10_000) -> None:
        """
        Replay historical data as fast as possible.
        Used for backfilling / testing.
        """
        logger.info(f"⚡ Burst replay: {num_orders:,} orders")

        for i, order in enumerate(self.gen.generate_orders_batch(num_orders)):
            if not self._running:
                break

            payload = json.dumps(order).encode("utf-8")
            self.producer.produce(
                topic="orders",
                key=self.string_serializer(order["customer_id"]),
                value=payload,
                on_delivery=self._delivery_report("orders"),
            )

            if i % 1000 == 0:
                self.producer.poll(0)
                logger.info(f"  Replayed {i:,}/{num_orders:,} orders")

        self.producer.flush(timeout=60)
        logger.info("✅ Burst replay complete")
        self._log_stats()

    def _log_stats(self) -> None:
        logger.info("📊 Producer Stats:")
        for topic, stats in self._stats.items():
            logger.info(f"   {topic}: {stats['produced']:,} produced, "
                        f"{stats['errors']} errors")


# ── ENTRYPOINT ────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",            choices=["live", "burst"], default="live")
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--schema-registry",   default=None)
    parser.add_argument("--num-customers",     type=int,   default=10_000)
    parser.add_argument("--num-orders",        type=int,   default=10_000)
    parser.add_argument("--orders-per-second", type=float, default=5.0)
    parser.add_argument("--clicks-per-second", type=float, default=50.0)
    args = parser.parse_args()

    producer = NexusFlowProducer(
        bootstrap_servers=args.bootstrap_servers,
        schema_registry_url=args.schema_registry,
        num_customers=args.num_customers,
    )

    if args.mode == "live":
        producer.run_live_stream(
            orders_per_second=args.orders_per_second,
            clicks_per_second=args.clicks_per_second,
        )
    else:
        producer.run_burst_replay(num_orders=args.num_orders)
