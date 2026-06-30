import pytest
from bronze_to_silver import clean_reviews_df  # pure transform, no Spark IO
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("t")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_clean_reviews_types_and_filter(spark):
    raw = spark.createDataFrame(
        [
            (
                "r1",
                "c1",
                "h" * 64,
                "SKU1",
                "Widget",
                "5",
                "good",
                "body",
                "2026-06-01",
                "true",
                "10",
                "2",
                "positive",
            ),
            (
                "r2",
                "c2",
                "h" * 64,
                "SKU2",
                "Gadget",
                "0",
                "bad",
                "body",
                "2026-06-01",
                "false",
                "1",
                "0",
                "negative",
            ),
        ],
        [
            "review_id",
            "customer_id",
            "customer_email_hash",
            "product_sku",
            "product_name",
            "rating",
            "title",
            "body",
            "review_date",
            "verified_purchase",
            "helpful_votes",
            "images_count",
            "sentiment",
        ],
    )
    out = clean_reviews_df(raw)
    rows = {r["review_id"]: r for r in out.collect()}
    # rating cast to int; out-of-range (0) row dropped
    assert "r2" not in rows
    assert rows["r1"]["rating"] == 5
    assert rows["r1"]["verified_purchase"] is True
    assert rows["r1"]["helpful_votes"] == 10
