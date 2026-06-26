"""
NexusFlow — PySpark Bronze → Silver Processing
================================================
Reads raw data from S3 bronze layer, applies:
  - Schema enforcement + validation
  - PII masking / tokenization
  - Deduplication
  - SCD Type 2 for customer dimension
  - Data type casting + normalization
  - Write to S3 silver layer (Parquet, partitioned)

Run on EMR Serverless or as EKS Spark operator job.
"""

import sys                                              # For command-line argument parsing
import logging                                          # For structured logging
import hashlib                                          # For hashing PII
from datetime import datetime                     # For date parsing and manipulation
from pyspark.sql import SparkSession, DataFrame, Window # For Spark session and window functions
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType, ArrayType,
    MapType, LongType, DoubleType
)

# ── LOGGING ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s" # Log format with timestamp, level, logger name, and message
)
logger = logging.getLogger("nexusflow.bronze_to_silver") 

# ── SCHEMAS ───────────────────────────────────────────────

ORDER_ITEM_SCHEMA = StructType([
    StructField("order_item_id",    StringType(),  True),
    StructField("product_sku",      StringType(),  True),
    StructField("product_name",     StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("subcategory",      StringType(),  True),
    StructField("quantity",         IntegerType(), True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("discount_amount",  DoubleType(),  True),
    StructField("total_price",      DoubleType(),  True),
    StructField("warehouse_id",     StringType(),  True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id",                    StringType(),            False),
    StructField("customer_id",                 StringType(),            False),
    StructField("order_status",                StringType(),            True),
    StructField("order_date",                  StringType(),            True),
    StructField("updated_at",                  StringType(),            True),
    StructField("items",                       ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("subtotal",                    DoubleType(),            True),
    StructField("discount_total",              DoubleType(),            True),
    StructField("shipping_fee",                DoubleType(),            True),
    StructField("tax_amount",                  DoubleType(),            True),
    StructField("total_amount",                DoubleType(),            True),
    StructField("payment_method",              StringType(),            True),
    StructField("payment_status",              StringType(),            True),
    StructField("shipping_method",             StringType(),            True),
    StructField("shipping_address_city",       StringType(),            True),
    StructField("shipping_address_state",      StringType(),            True),
    StructField("shipping_address_country",    StringType(),            True),
    StructField("estimated_delivery_date",     StringType(),            True),
    StructField("actual_delivery_date",        StringType(),            True),
    StructField("session_id",                  StringType(),            True),
    StructField("utm_source",                  StringType(),            True),
    StructField("utm_medium",                  StringType(),            True),
    StructField("utm_campaign",                StringType(),            True),
    StructField("is_gift",                     BooleanType(),           True),
    StructField("notes",                       StringType(),            True),
    StructField("_ingestion_ts",               StringType(),            True),
    StructField("_source",                     StringType(),            True),
    # Spark's columnNameOfCorruptRecord only populates a column that's
    # part of the schema — process_orders() filters on this column, which
    # doesn't exist post-read unless declared here.
    StructField("_corrupt_record",             StringType(),            True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",        StringType(),  False),
    StructField("email",              StringType(),  True),   # Will be masked
    StructField("email_hash",         StringType(),  True),
    StructField("first_name",         StringType(),  True),   # Will be masked
    StructField("last_name",          StringType(),  True),   # Will be masked
    StructField("phone",              StringType(),  True),   # Will be masked
    StructField("phone_hash",         StringType(),  True),
    StructField("date_of_birth",      StringType(),  True),   # Will be truncated to year
    StructField("age_bracket",        StringType(),  True),
    StructField("gender",             StringType(),  True),
    StructField("country",            StringType(),  True),
    StructField("city",               StringType(),  True),
    StructField("state",              StringType(),  True),
    StructField("postal_code",        StringType(),  True),   # Will be truncated to 3-digit
    StructField("registration_date",  StringType(),  True),
    StructField("loyalty_tier",       StringType(),  True),
    StructField("total_orders",       IntegerType(), True),
    StructField("ltv_usd",            DoubleType(),  True),
    StructField("is_email_verified",  BooleanType(), True),
    StructField("preferred_device",   StringType(),  True),
    StructField("segment",            StringType(),  True),
])

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",           StringType(),  False),
    StructField("session_id",         StringType(),  True),
    StructField("customer_id",        StringType(),  True),
    StructField("event_type",         StringType(),  True),
    StructField("event_ts",           StringType(),  True),
    StructField("page_url",           StringType(),  True),
    StructField("referrer_url",       StringType(),  True),
    StructField("product_sku",        StringType(),  True),
    StructField("product_category",   StringType(),  True),
    StructField("search_query",       StringType(),  True),
    StructField("device_type",        StringType(),  True),
    StructField("browser",            StringType(),  True),
    StructField("os",                 StringType(),  True),
    StructField("ip_hash",            StringType(),  True),   # Already hashed at source
    StructField("user_agent",         StringType(),  True),
    StructField("viewport_width",     IntegerType(), True),
    StructField("viewport_height",    IntegerType(), True),
    StructField("scroll_depth_pct",   IntegerType(), True),
    StructField("time_on_page_sec",   IntegerType(), True),
    StructField("utm_source",         StringType(),  True),
    StructField("utm_medium",         StringType(),  True),
    StructField("_ingestion_ts",      StringType(),  True),
    StructField("_partition_key",     StringType(),  True),
])


# ── PII MASKING UDFs ──────────────────────────────────────

@F.udf(StringType())                    # UDF to mask email addresses while keeping domain intact
def mask_email(email: str) -> str:
    """Keep domain, mask local part."""
    if email is None:
        return None
    parts = email.split("@")
    if len(parts) != 2:
        return "***@***"
    local = parts[0]
    masked = local[0] + "***" + (local[-1] if len(local) > 1 else "") if len(local) > 0 else "***"
    return f"{masked}@{parts[1]}"


@F.udf(StringType())
def truncate_postal(postal: str) -> str:
    """Truncate US postal to 3-digit prefix for k-anonymity."""
    if postal is None:
        return None
    clean = postal.replace("-", "").strip()
    return clean[:3] + "**" if len(clean) >= 3 else "***"


# ── PROCESSING FUNCTIONS ──────────────────────────────────

class BronzeToSilverProcessor:
    """Handles all bronze→silver transformations."""

    def __init__(self, spark: SparkSession, env: str,
                 bronze_bucket: str, silver_bucket: str):
        self.spark = spark
        self.env = env
        self.bronze_base = f"s3://{bronze_bucket}/bronze"
        self.silver_base = f"s3://{silver_bucket}/silver"
        self._metrics = {}

    def process_orders(self, date_partition: str) -> None:
        """Process raw orders JSON → clean Parquet."""
        logger.info(f"Processing orders for partition: {date_partition}")
        input_path = f"{self.bronze_base}/orders/date={date_partition}/*.json*"
        output_path = f"{self.silver_base}/orders/"

        # Read raw JSON
        df_raw = (
            self.spark.read
            .schema(ORDER_SCHEMA)
            .option("mode", "PERMISSIVE")   #PERMISSIVE — parse best-effort, put bad rows in a special column,DROPMALFORMED — silently discard bad rows,FAILFAST — throw an exception on first bad row
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(input_path)
        )

        # Spark 2.3+ disallows queries against a raw JSON/CSV source when
        # the only referenced column is the corrupt-record column (which
        # .filter(col("_corrupt_record")...) triggers) — cache materializes
        # the DataFrame so subsequent filters query the cached form instead
        # of replanning against the raw source.
        df_raw.cache()
        initial_count = df_raw.count()
        logger.info(f"  Raw orders read: {initial_count:,}")

        # Filter corrupt records (send to DLQ)          # dlq = dead letter queue, a common pattern for handling bad data
        df_corrupt = df_raw.filter(F.col("_corrupt_record").isNotNull())
        if df_corrupt.count() > 0:
            logger.warning(f"  Corrupt records: {df_corrupt.count():,}")
            df_corrupt.write.mode("append").json(
                f"{self.silver_base}/_dlq/orders/date={date_partition}/"
            )

        df = df_raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record") # Keep only well-formed records for processing,  drop null records

        # ── DEDUPLICATION ──
        window_dedup = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
        df = (
            df.withColumn("_row_num", F.row_number().over(window_dedup))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        # ── TYPE CASTING ──
        df = (
            df
            .withColumn("order_date",              F.to_timestamp("order_date"))
            .withColumn("updated_at",              F.to_timestamp("updated_at"))
            .withColumn("estimated_delivery_date", F.to_date("estimated_delivery_date"))
            .withColumn("actual_delivery_date",    F.to_date("actual_delivery_date"))
            .withColumn("_ingestion_ts",           F.to_timestamp("_ingestion_ts"))
        )

        # ── VALIDATION ──
        df = df.filter(
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("total_amount").isNotNull()
            & (F.col("total_amount") >= 0)
            & F.col("order_date").isNotNull()
            & F.col("order_status").isin(
                "pending", "confirmed", "processing",
                "shipped", "delivered", "cancelled", "refunded"
            )
        )

        # ── ENRICHMENT ──          # Add derived columns for easier analysis and partitioning
        df = (
            df
            .withColumn("order_year",      F.year("order_date"))
            .withColumn("order_month",     F.month("order_date"))
            .withColumn("order_day",       F.dayofmonth("order_date"))
            .withColumn("order_hour",      F.hour("order_date"))
            .withColumn("order_dayofweek", F.dayofweek("order_date"))
            .withColumn("is_weekend",      F.dayofweek("order_date").isin([1, 7]))
            .withColumn("item_count",      F.size("items"))
            .withColumn("has_discount",    F.col("discount_total") > 0)
            .withColumn("_processed_ts",   F.current_timestamp())
            .withColumn("_pipeline_version", F.lit("1.0.0"))
        )

        # ── WRITE SILVER ──
        final_count = df.count()
        logger.info(f"  Silver orders written: {final_count:,}")
        self._metrics["orders"] = {"input": initial_count, "output": final_count}

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("order_year", "order_month", "order_day")
            .option("compression", "snappy")
            .save(output_path)
        )
        logger.info(f"  ✅ Orders → {output_path}")

    def process_customers(self) -> None:
        """Process customer data with PII masking + SCD Type 2 prep."""
        logger.info("Processing customers with PII masking...")
        input_path = f"{self.bronze_base}/customers/*.jsonl"
        output_path = f"{self.silver_base}/customers/"

        df_raw = (
            self.spark.read
            .schema(CUSTOMER_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json(input_path)
        )

        # ── PII MASKING ──
        df = (
            df_raw
            # Remove raw PII
            .drop("email", "first_name", "last_name", "phone")
            # Truncate sensitive fields
            .withColumn("postal_code_prefix", truncate_postal(F.col("postal_code")))
            .drop("postal_code")
            # Keep only birth year
            .withColumn("birth_year", F.year(F.to_date("date_of_birth")))
            .drop("date_of_birth")
        )

        # ── TYPE CASTING ──
        df = (
            df
            .withColumn("registration_date", F.to_date("registration_date"))
            .withColumn("registration_year",  F.year("registration_date"))
            .withColumn("registration_month", F.month("registration_date"))
        )

        # ── SCD TYPE 2 METADATA ──
        df = (
            df
            .withColumn("_valid_from",       F.current_timestamp())
            .withColumn("_valid_to",         F.lit(None).cast(TimestampType()))
            .withColumn("_is_current",       F.lit(True))       
            .withColumn("_record_hash",      F.md5(F.concat_ws("|",
                F.col("customer_id"),
                F.col("loyalty_tier"),
                F.col("segment"),
                F.col("total_orders").cast(StringType()),
            )))
            .withColumn("_processed_ts",     F.current_timestamp())
        )

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("registration_year", "registration_month")
            .option("compression", "snappy")
            .save(output_path)
        )
        logger.info(f"  ✅ Customers → {output_path}, count: {df.count():,}")

    def process_clickstream(self, date_partition: str) -> None:
        """Process clickstream events → partitioned Parquet."""
        logger.info(f"Processing clickstream for: {date_partition}")
        input_path = f"{self.bronze_base}/clickstream/date={date_partition}/*"
        output_path = f"{self.silver_base}/clickstream/"

        df = (
            self.spark.read
            .schema(CLICKSTREAM_SCHEMA)
            .option("mode", "PERMISSIVE")
            .json(input_path)
        )

        # ── DEDUP ──
        df = df.dropDuplicates(["event_id"]) # Assuming event_id is unique, this will remove any duplicate events that may have been ingested multiple times due to retries or duplicates in the source data.

        # ── TYPE CASTING ──
        df = ( 
            df
            .withColumn("event_ts",      F.to_timestamp("event_ts"))
            .withColumn("_ingestion_ts", F.to_timestamp("_ingestion_ts"))
            .withColumn("event_date",    F.to_date("event_ts"))
            .withColumn("event_hour",    F.hour("event_ts"))
        )

        # ── SESSIONIZATION: watermark for late events ──
        # Note: Full streaming version uses Spark Structured Streaming
        df = (
            df
            .withColumn("event_year",  F.year("event_ts"))
            .withColumn("event_month", F.month("event_ts"))
            .withColumn("event_day",   F.dayofmonth("event_ts"))
        )

        # ── FILTER ──
        df = df.filter(
            F.col("event_id").isNotNull()
            & F.col("session_id").isNotNull()
            & F.col("event_ts").isNotNull()
            & F.col("event_type").isin(
                "page_view", "product_view", "add_to_cart", "remove_from_cart",
                "checkout_start", "search", "wishlist_add", "review_view", "click"
            )
        )

        df = df.withColumn("_processed_ts", F.current_timestamp())

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("event_year", "event_month", "event_day")
            .option("compression", "snappy")
            .save(output_path)
        )
        logger.info(f"  ✅ Clickstream → {output_path}")

    def process_inventory(self, snapshot_date: str) -> None:
        """Process inventory CSV snapshots."""
        logger.info(f"Processing inventory snapshot: {snapshot_date}")
        input_path = f"{self.bronze_base}/inventory/{snapshot_date}/*.csv"
        output_path = f"{self.silver_base}/inventory/"

        df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(input_path)
        )

        df = (
            df
            .withColumn("snapshot_ts",         F.to_timestamp("snapshot_ts"))
            .withColumn("quantity_on_hand",    F.col("quantity_on_hand").cast(IntegerType()))
            .withColumn("quantity_reserved",   F.col("quantity_reserved").cast(IntegerType()))
            .withColumn("quantity_available",  F.col("quantity_available").cast(IntegerType()))
            .withColumn("reorder_point",       F.col("reorder_point").cast(IntegerType()))
            .withColumn("reorder_quantity",    F.col("reorder_quantity").cast(IntegerType()))
            .withColumn("unit_cost",           F.col("unit_cost").cast(DoubleType()))
            .withColumn("last_received_date",  F.to_date("last_received_date"))
            # Derived
            .withColumn("is_low_stock",        F.col("quantity_available") <= F.col("reorder_point"))
            .withColumn("is_out_of_stock",     F.col("quantity_available") == 0)
            .withColumn("snapshot_date",       F.to_date("snapshot_ts"))
            .withColumn("snapshot_year",       F.year("snapshot_ts"))
            .withColumn("snapshot_month",      F.month("snapshot_ts"))
            .withColumn("_processed_ts",       F.current_timestamp())
        )

        (
            df.write
            .format("parquet")
            .mode("overwrite")
            .partitionBy("snapshot_year", "snapshot_month", "snapshot_date")
            .option("compression", "snappy")
            .save(output_path)
        )
        logger.info(f"  ✅ Inventory → {output_path}")

    # ── METRICS LOGGING / Pipeline Quality report ──
    def log_metrics(self) -> None:
        logger.info("📊 Processing Metrics:")
        for entity, m in self._metrics.items():
            if m:
                pct = (m.get("output", 0) / m.get("input", 1)) * 100
                logger.info(f"  {entity}: {m.get('input', 0):,} in → "
                            f"{m.get('output', 0):,} out ({pct:.1f}% pass rate)")


# ── SPARK SESSION + ENTRYPOINT ────────────────────────────

def create_spark_session(app_name: str, aws_region: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")       # Enable adaptive query execution for better performance on large datasets with skew or varying partition sizes
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Dynamically coalesce shuffle partitions based on runtime statistics to reduce small files and improve downstream performance
        .config("spark.sql.adaptive.skewJoin.enabled", "true")  # Automatically handle skewed joins by splitting large partitions to avoid OOM errors and improve join performance
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")  # Set a reasonable default for shuffle partitions; can be tuned based on cluster size and data volume
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # Use S3A for better performance and support for larger files when reading/writing to S3
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")    # Use IAM roles for authentication when running on AWS (e.g., EMR, EKS with IRSA)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") # Use Kryo serialization for better performance when shuffling complex objects
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")  # for Delta Lake silver layer, configure the catalog to use Delta
        .getOrCreate()
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bronze → Silver Processor")
    parser.add_argument("--env",            required=True)
    parser.add_argument("--bronze-bucket",  required=True)
    parser.add_argument("--silver-bucket",  required=True)
    parser.add_argument("--date-partition", required=True, help="YYYY-MM-DD")
    parser.add_argument("--entities",
                        nargs="+",
                        default=["orders", "customers", "clickstream", "inventory"],
                        choices=["orders", "customers", "clickstream", "inventory"])
    parser.add_argument("--aws-region", default="ca-central-1")
    args = parser.parse_args()

    spark = create_spark_session("nexusflow-bronze-to-silver", args.aws_region)

    processor = BronzeToSilverProcessor(
        spark=spark,
        env=args.env,
        bronze_bucket=args.bronze_bucket,
        silver_bucket=args.silver_bucket,
    )

    if "orders" in args.entities:
        processor.process_orders(args.date_partition)

    if "customers" in args.entities:
        processor.process_customers()

    if "clickstream" in args.entities:
        processor.process_clickstream(args.date_partition)

    if "inventory" in args.entities:
        processor.process_inventory(args.date_partition)

    processor.log_metrics()
    spark.stop()
    logger.info("✅ Bronze → Silver processing complete")
