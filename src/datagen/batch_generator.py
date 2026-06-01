"""
NexusFlow — Batch Generator
============================
Generates bulk e-commerce data files and uploads
directly to S3 bronze layer without going through Kafka.

Handles:
  - Inventory snapshots    → CSV
  - Product reviews        → XML
  - Customer master data   → JSONL
  - Historical orders      → JSONL (backfill)

Run modes:
  - local:  writes to /tmp/nexusflow-data/
  - s3:     writes directly to S3 bronze layer

Used by:
  - Kubernetes CronJob (daily at 01:00 UTC)
  - Manual backfill runs
  - Initial data seeding
"""

import os                                # For file handling and path manipulation
import sys                               # For adding parent directory to path for imports
import json                              # For JSON serialization            
import logging                           # For logging progress and info 
import argparse                          # For command-line argument parsing
import boto3                             # For S3 interactions (uploading files)
from datetime import datetime, date, timezone      # For timestamping and date partitioning
from io import StringIO, BytesIO         # For in-memory file handling (e.g., CSV generation)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ecommerce_generator import EcommerceDataGenerator

# ── LOGGING ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("nexusflow.batch_generator")


class S3BatchUploader:
    """Handles uploading generated data files to S3 bronze layer."""

    def __init__(self, bucket: str, region: str = "ca-central-1"):
        self.bucket   = bucket
        self.region   = region
        self.s3client = boto3.client("s3", region_name=region)
        self.today    = date.today().isoformat()
        logger.info(f"S3BatchUploader initialized — bucket: {bucket}")

    def _bronze_key(self, entity: str, filename: str) -> str:
        """Build S3 key with date partition."""
        return f"bronze/{entity}/date={self.today}/{filename}"

    def upload_string(self,
                      content: str,
                      entity: str,
                      filename: str,
                      content_type: str = "application/json") -> str:
        """Upload string content to S3."""
        key = self._bronze_key(entity, filename)
        self.s3client.put_object(
            Bucket      = self.bucket,
            Key         = key,
            Body        = content.encode("utf-8"),
            ContentType = content_type,
            Metadata    = {
                "source":        "batch-generator",
                "generated-date": self.today,
                "entity":        entity,
            }
        )
        s3_path = f"s3://{self.bucket}/{key}"
        logger.info(f"  ✅ Uploaded: {s3_path}")
        return s3_path

    def upload_jsonl(self,
                     records: list,
                     entity: str,
                     filename: str) -> str:
        """Upload list of dicts as JSONL."""
        content = "\n".join(json.dumps(r) for r in records)
        return self.upload_string(
            content      = content,
            entity       = entity,
            filename     = filename,
            content_type = "application/x-ndjson"
        )


class NexusFlowBatchGenerator:
    """
    Orchestrates batch data generation and S3 upload.

    Generates all non-streaming data sources:
      - Customer master data (JSONL)
      - Historical orders backfill (JSONL)
      - Inventory snapshots (CSV)
      - Product reviews (XML)
    """

    def __init__(self,
                 num_customers: int = 10_000,
                 seed: int = 42,
                 mode: str = "s3",
                 output_dir: str = "/tmp/nexusflow-data",
                 s3_bucket: str = "nexusflow-dev-lakehouse",
                 aws_region: str = "ca-central-1"):

        self.mode       = mode
        self.output_dir = output_dir
        self.today      = date.today().isoformat()

        logger.info(f"Initializing generator — mode: {mode}, "
                    f"customers: {num_customers:,}, seed: {seed}")

        # Core data generator
        self.gen = EcommerceDataGenerator(
            num_customers = num_customers,
            seed          = seed
        )

        # S3 uploader (only in s3 mode)
        if mode == "s3":
            self.uploader = S3BatchUploader(
                bucket = s3_bucket,
                region = aws_region
            )
        else:
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs(
                f"{output_dir}/bronze/customers/date={self.today}",
                exist_ok=True
            )
            os.makedirs(
                f"{output_dir}/bronze/orders/date={self.today}",
                exist_ok=True
            )
            os.makedirs(
                f"{output_dir}/bronze/inventory/date={self.today}",
                exist_ok=True
            )
            os.makedirs(
                f"{output_dir}/bronze/reviews/date={self.today}",
                exist_ok=True
            )

    def run_customers(self) -> None:
        """Generate customer master data and upload."""
        logger.info("Generating customer master data...")
        from dataclasses import asdict

        records  = [asdict(c) for c in self.gen.customers]
        filename = f"customers_{self.today}.jsonl"

        if self.mode == "s3":
            self.uploader.upload_jsonl(
                records  = records,
                entity   = "customers",
                filename = filename
            )
        else:
            path = f"{self.output_dir}/bronze/customers/date={self.today}/{filename}"
            with open(path, "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")
            logger.info(f"  ✅ Written: {path}")

        logger.info(f"  Customers: {len(records):,} records")

    def run_orders(self, num_orders: int = 100_000) -> None:
        """Generate historical orders backfill and upload in chunks."""
        logger.info(f"Generating {num_orders:,} historical orders...")

        chunk_size = 10_000
        chunk_num  = 0
        buffer     = []

        for i, order in enumerate(
            self.gen.generate_orders_batch(num_orders=num_orders)
        ):
            buffer.append(order)

            if len(buffer) >= chunk_size:
                chunk_num += 1
                filename   = f"orders_{self.today}_chunk{chunk_num:04d}.jsonl"

                if self.mode == "s3":
                    self.uploader.upload_jsonl(
                        records  = buffer,
                        entity   = "orders",
                        filename = filename
                    )
                else:
                    path = (f"{self.output_dir}/bronze/orders/"
                            f"date={self.today}/{filename}")
                    with open(path, "w") as f:
                        for r in buffer:
                            f.write(json.dumps(r) + "\n")
                    logger.info(f"  ✅ Written: {path}")

                buffer = []
                logger.info(f"  Orders chunk {chunk_num}: "
                            f"{chunk_num * chunk_size:,}/{num_orders:,}")

        # Upload remaining records
        if buffer:
            chunk_num += 1
            filename   = f"orders_{self.today}_chunk{chunk_num:04d}.jsonl"
            if self.mode == "s3":
                self.uploader.upload_jsonl(
                    records  = buffer,
                    entity   = "orders",
                    filename = filename
                )
            else:
                path = (f"{self.output_dir}/bronze/orders/"
                        f"date={self.today}/{filename}")
                with open(path, "w") as f:
                    for r in buffer:
                        f.write(json.dumps(r) + "\n")

        logger.info(f"  ✅ Orders complete: {num_orders:,} records "
                    f"in {chunk_num} chunks")

    def run_inventory(self) -> None:
        """Generate inventory snapshot and upload as CSV."""
        logger.info("Generating inventory snapshot...")
        filename = f"inventory_{self.today}.csv"

        if self.mode == "s3":
            # Generate to temp file then upload
            tmp_path = f"/tmp/inventory_{self.today}.csv"
            self.gen.generate_inventory_csv(tmp_path)

            with open(tmp_path, "r") as f:
                content = f.read()

            self.uploader.upload_string(
                content      = content,
                entity       = "inventory",
                filename     = filename,
                content_type = "text/csv"
            )
            os.remove(tmp_path)
        else:
            path = (f"{self.output_dir}/bronze/inventory/"
                    f"date={self.today}/{filename}")
            self.gen.generate_inventory_csv(path)
            logger.info(f"  ✅ Written: {path}")

    def run_reviews(self, num_reviews: int = 10_000) -> None:
        """Generate product reviews and upload as XML."""
        logger.info(f"Generating {num_reviews:,} product reviews...")
        filename   = f"reviews_{self.today}.xml"
        xml_content = self.gen.generate_reviews_xml(num_reviews)

        if self.mode == "s3":
            self.uploader.upload_string(
                content      = xml_content,
                entity       = "reviews",
                filename     = filename,
                content_type = "application/xml"
            )
        else:
            path = (f"{self.output_dir}/bronze/reviews/"
                    f"date={self.today}/{filename}")
            with open(path, "w") as f:
                f.write(xml_content)
            logger.info(f"  ✅ Written: {path}")

    def run_all(self,
                num_orders: int  = 100_000,
                num_reviews: int = 10_000) -> None:
        """Run all batch generators in sequence."""
        start = datetime.now(timezone.utc)
        logger.info("=" * 60)   
        logger.info("NexusFlow Batch Generator — Starting full run")
        logger.info(f"  Mode:     {self.mode}")
        logger.info(f"  Date:     {self.today}")
        logger.info(f"  Orders:   {num_orders:,}")
        logger.info(f"  Reviews:  {num_reviews:,}")
        logger.info("=" * 60)

        self.run_customers()
        self.run_orders(num_orders)
        self.run_inventory()
        self.run_reviews(num_reviews)

        elapsed = (datetime.now(timezone.utc) - start).seconds
        logger.info("=" * 60)
        logger.info(f"✅ Batch generation complete in {elapsed}s")
        logger.info("=" * 60)


# ── ENTRYPOINT ────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NexusFlow Batch Data Generator"
    )
    parser.add_argument(
        "--mode",
        choices=["local", "s3"],
        default="local",
        help="Output mode: local filesystem or S3"
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/nexusflow-data",
        help="Local output directory (mode=local only)"
    )
    parser.add_argument(
        "--s3-bucket",
        default="nexusflow-dev-lakehouse",
        help="S3 bucket name (mode=s3 only)"
    )
    parser.add_argument(
        "--aws-region",
        default="ca-central-1",
        help="AWS region"
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=10_000,
        help="Number of customers to generate"
    )
    parser.add_argument(
        "--num-orders",
        type=int,
        default=100_000,
        help="Number of historical orders to generate"
    )
    parser.add_argument(
        "--num-reviews",
        type=int,
        default=10_000,
        help="Number of product reviews to generate"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility"
    )
    args = parser.parse_args()

    generator = NexusFlowBatchGenerator(
        num_customers = args.num_customers,
        seed          = args.seed,
        mode          = args.mode,
        output_dir    = args.output_dir,
        s3_bucket     = args.s3_bucket,
        aws_region    = args.aws_region,
    )

    generator.run_all(
        num_orders  = args.num_orders,
        num_reviews = args.num_reviews,
    )
