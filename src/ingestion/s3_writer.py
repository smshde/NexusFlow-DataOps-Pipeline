"""
NexusFlow — S3 Writer Utilities
=================================
Shared S3 write utilities used by kafka_consumer.py
and batch_generator.py.

Handles:
  - Partitioned path construction
  - Retry logic with exponential backoff
  - File format helpers (JSONL, CSV, XML, Parquet)
  - Metadata tagging
  - Data landing verification
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("nexusflow.s3_writer")


class S3Writer:
    """
    Production-grade S3 writer with retry logic.

    Features:
      - Automatic partitioning by date
      - Exponential backoff on failures
      - Metadata tagging on every object
      - Landing verification
    """

    def __init__(self, bucket: str, region: str = "ca-central-1", max_retries: int = 3):
        self.bucket = bucket
        self.region = region
        self.max_retries = max_retries
        self.s3client = boto3.client("s3", region_name=region)

    # ── PATH BUILDERS ─────────────────────────────────────

    def bronze_path(
        self, entity: str, filename: str, date_str: Optional[str] = None
    ) -> str:
        """
        Build bronze layer S3 key.

        Example:
          bronze/orders/date=2026-05-22/orders_20260522_001.jsonl
        """
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"bronze/{entity}/date={date_str}/{filename}"

    def silver_path(
        self, entity: str, filename: str, year: int, month: int, day: int
    ) -> str:
        """
        Build silver layer S3 key with year/month/day partition.

        Example:
          silver/orders/order_year=2026/order_month=5/order_day=22/part-0001.parquet
        """
        return (
            f"silver/{entity}/"
            f"order_year={year}/order_month={month}/order_day={day}/"
            f"{filename}"
        )

    # ── WRITE METHODS ─────────────────────────────────────

    def write_jsonl(
        self,
        records: List[dict],
        entity: str,
        filename: str,
        layer: str = "bronze",
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Write list of dicts as JSONL to S3."""
        content = "\n".join(json.dumps(r) for r in records)
        return self.write_bytes(
            content=content.encode("utf-8"),
            key=self.bronze_path(entity, filename),
            content_type="application/x-ndjson",
            metadata={
                "entity": entity,
                "layer": layer,
                "record-count": str(len(records)),
                **(metadata or {}),
            },
        )

    def write_csv(
        self,
        content: str,
        entity: str,
        filename: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Write CSV string to S3 bronze layer."""
        return self.write_bytes(
            content=content.encode("utf-8"),
            key=self.bronze_path(entity, filename),
            content_type="text/csv",
            metadata={"entity": entity, "layer": "bronze", **(metadata or {})},
        )

    def write_xml(
        self,
        content: str,
        entity: str,
        filename: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """Write XML string to S3 bronze layer."""
        return self.write_bytes(
            content=content.encode("utf-8"),
            key=self.bronze_path(entity, filename),
            content_type="application/xml",
            metadata={"entity": entity, "layer": "bronze", **(metadata or {})},
        )

    def write_bytes(
        self,
        content: bytes,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Core write method with retry logic.
        All other write methods call this.
        """
        base_metadata = {
            "written-at": datetime.utcnow().isoformat(),
            "pipeline": "nexusflow",
            "version": "1.0.0",
        }
        if metadata:
            base_metadata.update(metadata)

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                self.s3client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=content,
                    ContentType=content_type,
                    Metadata=base_metadata,
                    ServerSideEncryption="AES256",
                )
                s3_uri = f"s3://{self.bucket}/{key}"
                logger.debug(f"Written: {s3_uri}")
                return s3_uri

            except ClientError as e:
                last_error = e
                wait = 2**attempt  # exponential backoff: 2, 4, 8 seconds
                logger.warning(
                    f"S3 write failed (attempt {attempt}/{self.max_retries}): "
                    f"{e.response['Error']['Code']} — retrying in {wait}s"
                )
                time.sleep(wait)

        raise RuntimeError(
            f"S3 write failed after {self.max_retries} attempts: {last_error}"
        )

    # ── VERIFICATION ──────────────────────────────────────

    def verify_landing(self, prefix: str) -> Dict:
        """
        Verify data has landed in S3 for a given prefix.
        Returns count and total size of objects.

        Used by e2e validation script.
        """
        paginator = self.s3client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        total_files = 0
        total_bytes = 0

        for page in pages:
            for obj in page.get("Contents", []):
                total_files += 1
                total_bytes += obj["Size"]

        return {
            "prefix": prefix,
            "bucket": self.bucket,
            "total_files": total_files,
            "total_bytes": total_bytes,
            "total_mb": round(total_bytes / 1_048_576, 2),
            "has_data": total_files > 0,
        }

    def list_partitions(self, entity: str, layer: str = "bronze") -> List[str]:
        """List all date partitions for an entity."""
        prefix = f"{layer}/{entity}/date="
        paginator = self.s3client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/")

        partitions = []
        for page in pages:
            for cp in page.get("CommonPrefixes", []):
                partitions.append(cp["Prefix"])

        return sorted(partitions)

    def get_latest_partition(self, entity: str, layer: str = "bronze") -> Optional[str]:
        """Get the most recent date partition for an entity."""
        partitions = self.list_partitions(entity, layer)
        return partitions[-1] if partitions else None
