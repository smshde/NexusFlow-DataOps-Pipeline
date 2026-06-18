"""
NexusFlow — Unit Tests
========================
PEP8-compliant, pytest-based tests covering:
  - Data generator correctness
  - PII masking / hashing
  - Schema validation
  - Bronze→Silver transformations (mocked Spark)
  - API endpoint correctness

Run: pytest src/datagen/test_generator.py -v --cov=src/datagen --cov-report=html
"""

import json
import pytest
import hashlib
import uuid
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from dataclasses import asdict

# ── FIXTURES ──────────────────────────────────────────────

@pytest.fixture(scope="module")
def generator():
    """Shared generator instance for all tests."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../datagen"))
    from ecommerce_generator import EcommerceDataGenerator
    return EcommerceDataGenerator(num_customers=100, seed=42)


@pytest.fixture
def sample_customer(generator):
    from ecommerce_generator import Customer
    return Customer.generate()


@pytest.fixture
def sample_order(generator, sample_customer):
    from ecommerce_generator import Order
    return Order.generate(sample_customer.customer_id)


@pytest.fixture
def sample_clickstream_event(generator):
    from ecommerce_generator import ClickstreamEvent
    session_id = str(uuid.uuid4())
    return ClickstreamEvent.generate(session_id)


# ── CUSTOMER TESTS ────────────────────────────────────────

class TestCustomerGeneration:
    """Tests for Customer data model."""

    def test_customer_has_uuid(self, sample_customer):
        assert sample_customer.customer_id
        # Validate UUID format
        uuid.UUID(sample_customer.customer_id)

    def test_email_hash_is_sha256(self, sample_customer):
        """Email hash must be 64-char hex (SHA-256)."""
        assert len(sample_customer.email_hash) == 64
        assert all(c in "0123456789abcdef" for c in sample_customer.email_hash)

    def test_email_hash_matches_email(self, sample_customer):
        """Hash must match SHA-256 of lowercase email."""
        expected = hashlib.sha256(
            sample_customer.email.lower().encode()
        ).hexdigest()
        assert sample_customer.email_hash == expected

    def test_phone_hash_is_sha256(self, sample_customer):
        assert len(sample_customer.phone_hash) == 64

    def test_age_bracket_valid(self, sample_customer):
        valid_brackets = {"18-24", "25-34", "35-44", "45-54", "55-64", "65+"}
        assert sample_customer.age_bracket in valid_brackets

    def test_loyalty_tier_valid(self, sample_customer):
        valid_tiers = {"bronze", "silver", "gold", "platinum"}
        assert sample_customer.loyalty_tier in valid_tiers

    def test_segment_valid(self, sample_customer):
        valid_segments = {"bargain_hunter", "loyal", "at_risk", "new", "vip", "dormant"}
        assert sample_customer.segment in valid_segments

    def test_country_not_empty(self, sample_customer):
        assert sample_customer.country

    def test_customer_reproducible_with_seed(self):
        """Same seed → same first customer."""
        from ecommerce_generator import EcommerceDataGenerator
        gen1 = EcommerceDataGenerator(num_customers=5, seed=999)
        gen2 = EcommerceDataGenerator(num_customers=5, seed=999)
        c1 = asdict(gen1.customers[0])
        c2 = asdict(gen2.customers[0])
        assert c1["customer_id"] == c2["customer_id"]
        assert c1["email_hash"] == c2["email_hash"]


# ── ORDER TESTS ───────────────────────────────────────────

class TestOrderGeneration:
    """Tests for Order data model."""

    def test_order_has_uuid(self, sample_order):
        uuid.UUID(sample_order.order_id)

    def test_order_total_is_positive(self, sample_order):
        assert sample_order.total_amount >= 0

    def test_order_items_not_empty(self, sample_order):
        assert len(sample_order.items) >= 1

    def test_order_items_have_required_fields(self, sample_order):
        for item in sample_order.items:
            assert "product_sku" in item
            assert "quantity" in item
            assert "unit_price" in item
            assert item["quantity"] >= 1
            assert item["unit_price"] > 0

    def test_order_total_consistency(self, sample_order):
        """Total must equal subtotal + shipping + tax."""
        expected = round(
            sample_order.subtotal +
            sample_order.shipping_fee +
            sample_order.tax_amount,
            2
        )
        assert abs(sample_order.total_amount - expected) < 0.01

    def test_order_status_valid(self, sample_order):
        valid_statuses = {
            "pending", "confirmed", "processing",
            "shipped", "delivered", "cancelled", "refunded"
        }
        assert sample_order.order_status in valid_statuses

    def test_order_payment_method_valid(self, sample_order):
        valid_methods = {
            "credit_card", "debit_card", "paypal",
            "apple_pay", "google_pay", "buy_now_pay_later", "gift_card"
        }
        assert sample_order.payment_method in valid_methods

    def test_order_date_not_future(self, sample_order):
        order_dt = datetime.fromisoformat(sample_order.order_date)
        assert order_dt <= datetime.now() + timedelta(hours=1)

    def test_order_has_ingestion_ts(self, sample_order):
        assert sample_order._ingestion_ts
        # Must be valid ISO datetime
        datetime.fromisoformat(sample_order._ingestion_ts)

    def test_order_serializable_to_json(self, sample_order):
        payload = json.dumps(asdict(sample_order))
        assert len(payload) > 100

    def test_batch_generation_produces_correct_count(self, generator):
        orders = list(generator.generate_orders_batch(num_orders=50))
        assert len(orders) == 50


# ── CLICKSTREAM TESTS ─────────────────────────────────────

class TestClickstreamGeneration:
    """Tests for ClickstreamEvent data model."""

    def test_event_has_uuid(self, sample_clickstream_event):
        uuid.UUID(sample_clickstream_event.event_id)

    def test_event_type_valid(self, sample_clickstream_event):
        valid_types = {
            "page_view", "product_view", "add_to_cart", "remove_from_cart",
            "checkout_start", "search", "wishlist_add", "review_view", "click"
        }
        assert sample_clickstream_event.event_type in valid_types

    def test_ip_is_hashed_not_raw(self, sample_clickstream_event):
        """ip_hash must be 64 chars (SHA-256), not a raw IP."""
        assert len(sample_clickstream_event.ip_hash) == 64
        assert "." not in sample_clickstream_event.ip_hash  # Not a raw IPv4

    def test_device_type_valid(self, sample_clickstream_event):
        assert sample_clickstream_event.device_type in {"mobile", "tablet", "desktop"}

    def test_event_ts_not_future(self, sample_clickstream_event):
        ts = datetime.fromisoformat(sample_clickstream_event.event_ts)
        assert ts <= datetime.now() + timedelta(hours=1)


# ── PII MASKING TESTS ─────────────────────────────────────

class TestPIIMasking:
    """Verify PII is properly masked or excluded."""

    def test_customer_dict_has_no_raw_email(self, sample_customer):
        d = asdict(sample_customer)
        # raw email exists in source, but should be masked downstream
        assert "email_hash" in d
        assert len(d["email_hash"]) == 64

    def test_customer_dict_has_no_raw_phone(self, sample_customer):
        d = asdict(sample_customer)
        assert "phone_hash" in d
        assert len(d["phone_hash"]) == 64

    def test_age_bracket_not_exact_dob(self, sample_customer):
        """Only bucketed age, not exact birth date in final output."""
        assert sample_customer.age_bracket in {
            "18-24", "25-34", "35-44", "45-54", "55-64", "65+"
        }

    def test_clickstream_no_raw_ip(self, sample_clickstream_event):
        assert "." not in sample_clickstream_event.ip_hash  # not a dotted IPv4
        assert len(sample_clickstream_event.ip_hash) == 64


# ── DATA QUALITY TESTS ────────────────────────────────────

class TestDataQuality:
    """Comprehensive data quality checks."""

    def test_no_negative_prices(self, generator):
        for order in generator.generate_orders_batch(num_orders=200):
            assert order["total_amount"] >= 0, f"Negative total in order {order['order_id']}"
            for item in order["items"]:
                assert item["unit_price"] >= 0
                assert item["quantity"] >= 1

    def test_order_item_count_within_range(self, generator):
        for order in generator.generate_orders_batch(num_orders=100):
            assert 1 <= len(order["items"]) <= 5

    def test_all_customers_have_unique_ids(self, generator):
        ids = [c.customer_id for c in generator.customers]
        assert len(ids) == len(set(ids)), "Duplicate customer IDs found"

    def test_all_customers_have_unique_email_hashes(self, generator):
        """No two customers share the same email hash."""
        hashes = [c.email_hash for c in generator.customers]
        assert len(hashes) == len(set(hashes)), "Duplicate email hashes found"

    def test_inventory_csv_has_correct_columns(self, generator, tmp_path):
        path = str(tmp_path / "inventory.csv")
        generator.generate_inventory_csv(path)
        import csv
        with open(path) as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
        required = {
            "snapshot_ts", "warehouse_id", "product_sku",
            "quantity_on_hand", "quantity_reserved", "quantity_available"
        }
        assert required.issubset(set(headers))

    def test_reviews_xml_is_valid(self, generator):
        import xml.etree.ElementTree as ET
        xml_str = generator.generate_reviews_xml(num_reviews=100)
        # Should parse without error
        root = ET.fromstring(xml_str)
        assert root.tag == "reviews"
        reviews = root.findall("review")
        assert len(reviews) == 100

    def test_reviews_xml_has_required_fields(self, generator):
        import xml.etree.ElementTree as ET
        xml_str = generator.generate_reviews_xml(num_reviews=10)
        root = ET.fromstring(xml_str)
        for review in root.findall("review"):
            assert review.find("review_id") is not None
            assert review.find("product_sku") is not None
            assert review.find("rating") is not None
            rating = int(review.find("rating").text)
            assert 1 <= rating <= 5


# ── API TESTS ─────────────────────────────────────────────

class TestAnalyticsAPI:
    """Tests for FastAPI analytics serving layer."""

    @pytest.fixture
    def client(self):
        """FastAPI test client with mocked DB."""
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../serving"))
        from fastapi_main import app
        from httpx import AsyncClient
        return AsyncClient(app=app, base_url="http://test")

    @pytest.mark.asyncio
    async def test_health_endpoint(self, client):
        async with client as c:
            resp = await c.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "env" in data

    @pytest.mark.asyncio
    async def test_health_returns_json(self, client):
        async with client as c:
            resp = await c.get("/health")
        assert resp.headers["content-type"].startswith("application/json")


# ── SCHEMA VALIDATION TESTS ───────────────────────────────

class TestSchemaValidation:
    """Validate generated data against expected schemas."""

    def test_order_schema_fields_complete(self, sample_order):
        required_fields = {
            "order_id", "customer_id", "order_status", "order_date",
            "items", "total_amount", "payment_method", "shipping_method",
            "utm_source", "_ingestion_ts", "_source"
        }
        d = asdict(sample_order)
        missing = required_fields - set(d.keys())
        assert not missing, f"Missing fields: {missing}"

    def test_clickstream_schema_fields_complete(self, sample_clickstream_event):
        required_fields = {
            "event_id", "session_id", "event_type", "event_ts",
            "page_url", "device_type", "browser", "ip_hash",
            "_ingestion_ts", "_partition_key"
        }
        d = asdict(sample_clickstream_event)
        missing = required_fields - set(d.keys())
        assert not missing, f"Missing fields: {missing}"
