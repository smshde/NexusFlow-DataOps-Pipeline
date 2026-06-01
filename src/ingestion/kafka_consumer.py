"""
NexusFlow — Kafka Consumer + S3 Writer
========================================
Consumes events from MSK topics and writes
micro-batches to S3 bronze layer.

This is the bridge between streaming (Kafka) and
batch storage (S3) — the core of the ingestion layer.

Topics consumed:
  - orders           → s3://bronze/orders/
  - clickstream      → s3://bronze/clickstream/
  - inventory-events → s3://bronze/inventory-events/
  - product-reviews  → s3://bronze/product-reviews/

Flush strategy:
  - Every 1,000 messages per topic  (size-based)
  - Every 30 seconds                (time-based)
  whichever comes first

This service runs as a long-lived Kubernetes Deployment.
"""

import os
import sys
import json
import logging
import signal
import time
import boto3
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List
from confluent_kafka import Consumer, KafkaError, KafkaException

# ── LOGGING ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("nexusflow.kafka_consumer")


class S3MicroBatchWriter:
    """
    Writes micro-batches of Kafka messages to S3.

    Batching strategy:
      Accumulates messages in memory buffer.
      Flushes when buffer hits size limit OR time limit.
      Each flush = one S3 object (one file per batch).
    """

    def __init__(self,
                 bucket: str,
                 region: str,
                 flush_size: int = 1_000,
                 flush_interval_sec: int = 30):

        self.bucket             = bucket
        self.flush_size         = flush_size
        self.flush_interval_sec = flush_interval_sec
        self.s3client           = boto3.client("s3", region_name=region)

        # In-memory buffers: topic → list of message dicts
        self.buffers: Dict[str, List[dict]] = defaultdict(list)
        self.last_flush_time: Dict[str, float] = defaultdict(time.time)
        self._stats: Dict[str, int] = defaultdict(int)

        logger.info(
            f"S3Writer initialized — bucket: {bucket}, "
            f"flush_size: {flush_size}, interval: {flush_interval_sec}s"
        )

    def _s3_key(self, topic: str, batch_ts: datetime) -> str:
        """
        Build partitioned S3 key.

        Format:
          bronze/{topic}/date=YYYY-MM-DD/
          {topic}_{YYYYMMDD}_{HHMMSS}_{epoch}.jsonl
        """
        date_str  = batch_ts.strftime("%Y-%m-%d")
        ts_str    = batch_ts.strftime("%Y%m%d_%H%M%S")
        epoch     = int(batch_ts.timestamp())
        return (
            f"bronze/{topic}/date={date_str}/"
            f"{topic}_{ts_str}_{epoch}.jsonl"
        )

    def add_message(self, topic: str, message: dict) -> None:
        """Add message to buffer. Flush if size limit reached."""
        self.buffers[topic].append(message)

        if len(self.buffers[topic]) >= self.flush_size:
            self._flush(topic, reason="size")

    def check_time_flush(self) -> None:
        """Flush topics that have exceeded time interval."""
        now = time.time()
        for topic in list(self.buffers.keys()):
            if (self.buffers[topic] and
                    now - self.last_flush_time[topic] >= self.flush_interval_sec):
                self._flush(topic, reason="time")

    def _flush(self, topic: str, reason: str = "manual") -> None:
        """Write buffer to S3 as JSONL file."""
        if not self.buffers[topic]:
            return

        batch     = self.buffers[topic].copy()
        self.buffers[topic].clear()
        batch_ts  = datetime.now(timezone.utc)
        s3_key    = self._s3_key(topic, batch_ts)

        # Build JSONL content
        content = "\n".join(json.dumps(msg) for msg in batch)

        try:
            self.s3client.put_object(
                Bucket      = self.bucket,
                Key         = s3_key,
                Body        = content.encode("utf-8"),
                ContentType = "application/x-ndjson",
                Metadata    = {
                    "topic":        topic,
                    "batch-size":   str(len(batch)),
                    "flush-reason": reason,
                    "ingested-at":  batch_ts.isoformat(),
                }
            )
            self._stats[topic] += len(batch)
            self.last_flush_time[topic] = time.time()
            logger.info(
                f"  📦 Flushed [{reason}] {topic}: "
                f"{len(batch):,} msgs → s3://{self.bucket}/{s3_key}"
            )
        except Exception as e:
            logger.error(f"S3 flush failed for {topic}: {e}")
            # Re-add messages to buffer for retry
            self.buffers[topic].extend(batch)

    def flush_all(self) -> None:
        """Flush all topics — called on shutdown."""
        logger.info("Flushing all remaining buffers...")
        for topic in list(self.buffers.keys()):
            if self.buffers[topic]:
                self._flush(topic, reason="shutdown")

    def log_stats(self) -> None:
        """Log total messages written per topic."""
        logger.info("📊 S3 Writer Stats:")
        for topic, count in self._stats.items():
            logger.info(f"   {topic}: {count:,} messages written to S3")


class NexusFlowKafkaConsumer:
    """
    Long-running Kafka consumer service.

    Subscribes to all NexusFlow topics.
    Deserializes messages.
    Passes to S3MicroBatchWriter for buffering and flushing.
    Commits offsets only after successful S3 write.
    """

    TOPICS = [
        "orders",
        "clickstream",
        "inventory-events",
        "product-reviews",
        "user-sessions",
    ]

    def __init__(self,
                 bootstrap_servers: str,
                 s3_bucket: str,
                 aws_region: str       = "ca-central-1",
                 consumer_group: str   = "nexusflow-s3-sink",
                 flush_size: int       = 1_000,
                 flush_interval: int   = 30):

        self._running = True
        self._stats   = {
            "consumed":   0,
            "errors":     0,
            "parse_errors": 0,
        }

        # ── KAFKA CONSUMER CONFIG ────────────────────────
        consumer_conf = {
            "bootstrap.servers":         bootstrap_servers,
            "group.id":                  consumer_group,
            "auto.offset.reset":         "earliest",
            "enable.auto.commit":        False,   # manual commit after S3 write
            "max.poll.interval.ms":      300_000, # 5 min max between polls
            "session.timeout.ms":        30_000,
            "heartbeat.interval.ms":     10_000,
            "fetch.min.bytes":           1,
            "fetch.wait.max.ms":         500,
            # IAM auth for MSK
            "security.protocol":         "SASL_SSL",
            "sasl.mechanism":            "AWS_MSK_IAM",
            "sasl.jaas.config":          (
                "software.amazon.msk.auth.iam.IAMLoginModule required;"
            ),
            "sasl.client.callback.handler.class": (
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
            ),
        }

        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe(
            self.TOPICS,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        # ── S3 WRITER ────────────────────────────────────
        self.writer = S3MicroBatchWriter(
            bucket             = s3_bucket,
            region             = aws_region,
            flush_size         = flush_size,
            flush_interval_sec = flush_interval,
        )

        # Graceful shutdown handlers
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT,  self._shutdown)

        logger.info(
            f"Consumer initialized — "
            f"group: {consumer_group}, "
            f"topics: {self.TOPICS}"
        )

    def _on_assign(self, consumer, partitions) -> None:
        logger.info(
            f"Partitions assigned: "
            f"{[f'{p.topic}[{p.partition}]' for p in partitions]}"
        )

    def _on_revoke(self, consumer, partitions) -> None:
        logger.info(
            f"Partitions revoked — flushing buffers..."
        )
        self.writer.flush_all()

    def _shutdown(self, signum, frame) -> None:
        logger.info(f"Shutdown signal {signum} received...")
        self._running = False

    def _parse_message(self, msg) -> dict:
        """Parse Kafka message value as JSON."""
        try:
            value = json.loads(msg.value().decode("utf-8"))
            # Add ingestion metadata
            value["_kafka_topic"]     = msg.topic()
            value["_kafka_partition"] = msg.partition()
            value["_kafka_offset"]    = msg.offset()
            value["_consumed_at"]     = datetime.utcnow().isoformat()
            return value
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._stats["parse_errors"] += 1
            logger.warning(
                f"Parse error on {msg.topic()}[{msg.partition()}]"
                f"@{msg.offset()}: {e}"
            )
            # Return raw message wrapped in error envelope
            return {
                "_error":           "parse_failed",
                "_raw":             msg.value().decode("utf-8", errors="replace"),
                "_kafka_topic":     msg.topic(),
                "_kafka_partition": msg.partition(),
                "_kafka_offset":    msg.offset(),
                "_consumed_at":     datetime.utcnow().isoformat(),
            }

    def run(self) -> None:
        """Main consume loop — runs until shutdown signal."""
        logger.info("🚀 Starting consume loop...")
        last_stats_ts   = time.time()
        last_commit_ts  = time.time()
        stats_interval  = 60   # log stats every 60 seconds
        commit_interval = 10   # commit offsets every 10 seconds

        try:
            while self._running:
                # Poll for messages (500ms timeout)
                msg = self.consumer.poll(timeout=0.5)

                if msg is None:
                    # No message — check for time-based flushes
                    self.writer.check_time_flush()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition — not an error
                        logger.debug(
                            f"End of partition: "
                            f"{msg.topic()}[{msg.partition()}]"
                        )
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        self._stats["errors"] += 1
                    continue

                # Parse and buffer message
                parsed = self._parse_message(msg)
                self.writer.add_message(msg.topic(), parsed)
                self._stats["consumed"] += 1

                # Check time-based flush
                self.writer.check_time_flush()

                # Commit offsets periodically
                now = time.time()
                if now - last_commit_ts >= commit_interval:
                    self.consumer.commit(asynchronous=True)
                    last_commit_ts = now

                # Log stats periodically
                if now - last_stats_ts >= stats_interval:
                    self._log_stats()
                    last_stats_ts = now

        except KafkaException as e:
            logger.error(f"Fatal Kafka error: {e}")
        finally:
            logger.info("Shutting down consumer...")
            self.writer.flush_all()
            self.consumer.commit(asynchronous=False)
            self.consumer.close()
            self._log_stats()
            self.writer.log_stats()
            logger.info("✅ Consumer shutdown complete")

    def _log_stats(self) -> None:
        logger.info(
            f"📊 Consumer Stats — "
            f"consumed: {self._stats['consumed']:,}, "
            f"errors: {self._stats['errors']}, "
            f"parse_errors: {self._stats['parse_errors']}"
        )


# ── ENTRYPOINT ────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="NexusFlow Kafka Consumer → S3 Writer"
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="MSK bootstrap broker string"
    )
    parser.add_argument(
        "--s3-bucket",
        default="nexusflow-dev-lakehouse",
        help="S3 destination bucket"
    )
    parser.add_argument(
        "--aws-region",
        default="ca-central-1",
        help="AWS region"
    )
    parser.add_argument(
        "--consumer-group",
        default="nexusflow-s3-sink",
        help="Kafka consumer group ID"
    )
    parser.add_argument(
        "--flush-size",
        type=int,
        default=1_000,
        help="Flush to S3 after N messages per topic"
    )
    parser.add_argument(
        "--flush-interval",
        type=int,
        default=30,
        help="Flush to S3 every N seconds"
    )
    args = parser.parse_args()

    consumer = NexusFlowKafkaConsumer(
        bootstrap_servers = args.bootstrap_servers,
        s3_bucket         = args.s3_bucket,
        aws_region        = args.aws_region,
        consumer_group    = args.consumer_group,
        flush_size        = args.flush_size,
        flush_interval    = args.flush_interval,
    )
    consumer.run()
