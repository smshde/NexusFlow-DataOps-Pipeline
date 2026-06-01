"""
NexusFlow — E-Commerce Data Generator
======================================
Generates realistic, reproducible fake e-commerce data for:
  - Orders (JSON, structured)
  - Clickstream events (Avro, semi-structured)
  - Product reviews (XML, semi-structured)
  - Inventory snapshots (CSV, structured)
  - User sessions (nested JSON, semi-structured)

Supports both batch file generation and streaming to Kafka.
"""

import json                                       # For JSON serialization
import random                                     # For random data generation
import uuid                                       # For unique identifiers
import csv                                        # For CSV writing 
import xml.etree.ElementTree as ET                # For XML generation
from datetime import datetime, timedelta, timezone # For date handling
from dataclasses import dataclass, asdict, field  # For data modeling                                       
from typing import Optional, Generator      # For type hints and generator functions
from faker import Faker                     # For realistic fake data generation
import hashlib                              # For hashing PII data           

fake = Faker()  # Initialize Faker instance
Faker.seed(42)  # Reproducible data
random.seed(42) # Reproducible random choices


# ── REFERENCE DATA ─────────────────────────────────────────

PRODUCT_CATALOG = [
    {"sku": f"SKU-{i:05d}", "name": fake.catch_phrase(), "category": cat,
     "subcategory": sub, "price": round(random.uniform(9.99, 499.99), 2),
     "cost": round(random.uniform(3.00, 200.00), 2)}
    for i, (cat, sub) in enumerate([
        ("Electronics", "Smartphones"), ("Electronics", "Laptops"),
        ("Electronics", "Headphones"), ("Electronics", "Tablets"),
        ("Clothing", "Men's Tops"), ("Clothing", "Women's Tops"),
        ("Clothing", "Footwear"), ("Clothing", "Accessories"),
        ("Home", "Kitchen"), ("Home", "Furniture"), ("Home", "Decor"),
        ("Sports", "Fitness"), ("Sports", "Outdoor"), ("Sports", "Team Sports"),
        ("Books", "Fiction"), ("Books", "Non-Fiction"), ("Books", "Technical"),
        ("Beauty", "Skincare"), ("Beauty", "Makeup"), ("Beauty", "Fragrance"),
        ("Grocery", "Snacks"), ("Grocery", "Beverages"), ("Grocery", "Organic"),
        ("Toys", "Board Games"), ("Toys", "Action Figures"), ("Toys", "Educational"),
    ] * 20)
]

WAREHOUSES = [
    {"id": "WH-NYC", "city": "New York",     "lat": 40.7128, "lon": -74.0060},
    {"id": "WH-LAX", "city": "Los Angeles",  "lat": 34.0522, "lon": -118.2437},
    {"id": "WH-CHI", "city": "Chicago",      "lat": 41.8781, "lon": -87.6298},
    {"id": "WH-HOU", "city": "Houston",      "lat": 29.7604, "lon": -95.3698},
    {"id": "WH-DFW", "city": "Dallas",       "lat": 32.7767, "lon": -96.7970},
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay",
                   "google_pay", "buy_now_pay_later", "gift_card"]

SHIPPING_METHODS = ["standard", "express", "overnight", "pickup", "same_day"]

DEVICE_TYPES = ["mobile", "tablet", "desktop"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
OS_LIST = ["iOS", "Android", "Windows", "macOS", "Linux"]
UTM_SOURCES = ["google", "facebook", "instagram", "email", "direct",
               "bing", "tiktok", "youtube", "affiliate"] #  Urchin Tracking Module to track where traffic is coming from
                                                            # who sent the traffic?
UTM_MEDIUMS = ["cpc", "organic", "social", "email", "referral", "none"] # what type of channel is sending the traffic? Cost Per Click, Organic Search, Social Media, Email, Referral, None


# ── DATA MODELS ───────────────────────────────────────────

@dataclass
class Customer:
    customer_id: str
    email: str                    # Will be hashed as PII
    email_hash: str               # SHA-256 of email (safe)
    first_name: str               # PII
    last_name: str                # PII
    phone: str                    # PII
    phone_hash: str
    date_of_birth: str            # PII
    age_bracket: str              # Safe bucketed version
    gender: str
    country: str
    city: str
    state: str
    postal_code: str
    registration_date: str
    loyalty_tier: str
    total_orders: int
    ltv_usd: float                  # Lifetime Value in USD
    is_email_verified: bool
    preferred_device: str
    segment: str

    @classmethod
    def generate(cls) -> "Customer":
        email = fake.email()
        phone = fake.phone_number()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
        age = (datetime.now().date() - dob).days // 365
        total_orders = random.randint(0, 200)
        ltv = round(total_orders * random.uniform(30, 250), 2)

        return cls(
            customer_id=str(uuid.uuid4()),
            email=email,
            email_hash=hashlib.sha256(email.lower().encode()).hexdigest(),
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            phone=phone,
            phone_hash=hashlib.sha256(phone.encode()).hexdigest(),
            date_of_birth=dob.isoformat(),
            age_bracket=cls._age_bracket(age),
            gender=random.choice(["M", "F", "NB", "prefer_not_to_say"]),
            country=random.choice(["US"] * 70 + ["CA"] * 10 + ["GB"] * 8 + ["AU"] * 5 + ["DE"] * 4 + ["FR"] * 3),
            city=fake.city(),
            state=fake.state_abbr(),
            postal_code=fake.zipcode(),
            registration_date=fake.date_between(start_date="-5y", end_date="today").isoformat(),
            loyalty_tier=random.choice(["bronze"] * 50 + ["silver"] * 30 + ["gold"] * 15 + ["platinum"] * 5),
            total_orders=total_orders,
            ltv_usd=ltv,
            is_email_verified=random.random() > 0.15,
            preferred_device=random.choice(DEVICE_TYPES),
            segment=random.choice(["bargain_hunter", "loyal", "at_risk", "new", "vip", "dormant"]),
        )

    @staticmethod
    def _age_bracket(age: int) -> str:
        if age < 25: return "18-24"
        if age < 35: return "25-34"
        if age < 45: return "35-44"
        if age < 55: return "45-54"
        if age < 65: return "55-64"
        return "65+"


@dataclass
class OrderItem:
    order_item_id: str
    product_sku: str
    product_name: str
    category: str
    subcategory: str
    quantity: int
    unit_price: float
    discount_amount: float
    total_price: float
    warehouse_id: str


@dataclass
class Order:
    order_id: str
    customer_id: str
    order_status: str
    order_date: str
    updated_at: str
    items: list
    subtotal: float
    discount_total: float
    shipping_fee: float
    tax_amount: float
    total_amount: float
    payment_method: str
    payment_status: str
    shipping_method: str
    shipping_address_city: str
    shipping_address_state: str
    shipping_address_country: str
    estimated_delivery_date: str
    actual_delivery_date: Optional[str]
    session_id: str
    utm_source: str
    utm_medium: str
    utm_campaign: str
    is_gift: bool
    notes: Optional[str]
    # Metadata
    _ingestion_ts: str
    _source: str

    @classmethod
    def generate(cls, customer_id: str, order_date: Optional[datetime] = None) -> "Order":
        if order_date is None:
            order_date = fake.date_time_between(start_date="-2y", end_date="now")

        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 25, 20, 10, 5])[0]
        products = random.sample(PRODUCT_CATALOG, num_items)

        items = []
        for p in products:
            qty = random.randint(1, 5)
            discount = round(p["price"] * random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
            items.append(OrderItem(
                order_item_id=str(uuid.uuid4()),
                product_sku=p["sku"],
                product_name=p["name"],
                category=p["category"],
                subcategory=p["subcategory"],
                quantity=qty,
                unit_price=p["price"],
                discount_amount=discount,
                total_price=round((p["price"] - discount) * qty, 2),
                warehouse_id=random.choice(WAREHOUSES)["id"],
            ))

        subtotal = sum(i.total_price for i in items)
        discount_total = sum(i.discount_amount * i.quantity for i in items)
        shipping_fee = round(random.choice([0, 0, 5.99, 9.99, 14.99]), 2)
        tax = round(subtotal * 0.08, 2)
        total = round(subtotal + shipping_fee + tax, 2)

        status = random.choices(
            ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"],
            weights=[5, 10, 15, 20, 40, 7, 3]
        )[0]

        delivery_date = order_date + timedelta(days=random.randint(2, 14))
        actual_delivery = (
            delivery_date + timedelta(days=random.randint(-2, 3))
            if status == "delivered" else None
        )

        return cls(
            order_id=str(uuid.uuid4()),
            customer_id=customer_id,
            order_status=status,
            order_date=order_date.isoformat(),
            updated_at=datetime.now().isoformat(),
            items=[asdict(i) for i in items],
            subtotal=round(subtotal, 2),
            discount_total=round(discount_total, 2),
            shipping_fee=shipping_fee,
            tax_amount=tax,
            total_amount=total,
            payment_method=random.choice(PAYMENT_METHODS),
            payment_status="completed" if status not in ["pending"] else "pending",
            shipping_method=random.choice(SHIPPING_METHODS),
            shipping_address_city=fake.city(),
            shipping_address_state=fake.state_abbr(),
            shipping_address_country="US",
            estimated_delivery_date=delivery_date.date().isoformat(),
            actual_delivery_date=actual_delivery.date().isoformat() if actual_delivery else None,
            session_id=str(uuid.uuid4()),
            utm_source=random.choice(UTM_SOURCES),
            utm_medium=random.choice(UTM_MEDIUMS),
            utm_campaign=f"camp_{fake.word()}_{random.randint(2023, 2026)}",
            is_gift=random.random() < 0.08,
            notes=fake.sentence() if random.random() < 0.05 else None,
            _ingestion_ts=datetime.now(timezone.utc).isoformat(),
            _source="order-service-v2",
        )


@dataclass
class ClickstreamEvent:
    event_id: str
    session_id: str
    customer_id: Optional[str]
    event_type: str
    event_ts: str
    page_url: str
    referrer_url: Optional[str]         # Attribution: From where did the user come to this page?
    product_sku: Optional[str]
    product_category: Optional[str]
    search_query: Optional[str]
    device_type: str
    browser: str
    os: str
    ip_hash: str          # Hashed for PII
    user_agent: str       # User agent (device/bot detection- Exact browser identity)
    viewport_width: int                 # Screen width in pixels (device/screen used)
    viewport_height: int                # Screen height in pixels
    scroll_depth_pct: Optional[int]     # Percentage of page scrolled (for page_view events/engagement)
    time_on_page_sec: Optional[int]     # Time spent on page in seconds (for page_view events)
    utm_source: Optional[str]
    utm_medium: Optional[str]
    _ingestion_ts: str       # Timestamp when event was generated (for data freshness and ordering) 
    _partition_key: str     # Key for partitioning in data lake or streaming platform (e.g., session_id or customer_id)--> kafka infrastructure

    @classmethod
    def generate(cls, session_id: str, customer_id: Optional[str] = None,
                 event_ts: Optional[datetime] = None) -> "ClickstreamEvent":
        if event_ts is None:
            event_ts = fake.date_time_between(start_date="-30d", end_date="now")

        event_type = random.choices(
            ["page_view", "product_view", "add_to_cart", "remove_from_cart",
             "checkout_start", "search", "wishlist_add", "review_view", "click"],
            weights=[30, 25, 15, 5, 5, 10, 3, 4, 3]
        )[0]

        product = random.choice(PRODUCT_CATALOG) if event_type in [
            "product_view", "add_to_cart", "remove_from_cart", "review_view"
        ] else None

        ip = fake.ipv4()
        return cls(
            event_id=str(uuid.uuid4()),
            session_id=session_id,
            customer_id=customer_id,
            event_type=event_type,
            event_ts=event_ts.isoformat(),
            page_url=f"https://shop.nexusflow.io/{fake.uri_path()}",
            referrer_url=fake.url() if random.random() > 0.4 else None,
            product_sku=product["sku"] if product else None,
            product_category=product["category"] if product else None,
            search_query=fake.catch_phrase() if event_type == "search" else None,
            device_type=random.choice(DEVICE_TYPES),
            browser=random.choice(BROWSERS),
            os=random.choice(OS_LIST),
            ip_hash=hashlib.sha256(ip.encode()).hexdigest(),
            user_agent=fake.user_agent(),
            viewport_width=random.choice([375, 768, 1280, 1440, 1920]),
            viewport_height=random.choice([667, 1024, 720, 900, 1080]),
            scroll_depth_pct=random.randint(10, 100) if event_type == "page_view" else None,
            time_on_page_sec=random.randint(5, 600) if event_type == "page_view" else None,
            utm_source=random.choice(UTM_SOURCES) if random.random() > 0.6 else None,
            utm_medium=random.choice(UTM_MEDIUMS) if random.random() > 0.6 else None,
            _ingestion_ts=datetime.now(timezone.utc).isoformat(),
            _partition_key=session_id,
        )


# ── GENERATORS ────────────────────────────────────────────

class EcommerceDataGenerator:
    """Main generator class — produces all data types."""

    def __init__(self, num_customers: int = 10_000, seed: int = 42):
        self.seed = seed
        random.seed(seed)
        Faker.seed(seed)
        self.customers = [Customer.generate() for _ in range(num_customers)]
        print(f"✅ Generated {len(self.customers)} customers")

    def generate_orders_batch(self,
                               num_orders: int = 100_000,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None) -> Generator:
        """Yield order dicts for batch processing."""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=730)
        if end_date is None:
            end_date = datetime.now()

        for _ in range(num_orders):
            customer = random.choice(self.customers)
            order_date = fake.date_time_between(start_date=start_date, end_date=end_date)
            order = Order.generate(customer.customer_id, order_date)
            yield asdict(order)

    def generate_clickstream_batch(self, num_events: int = 500_000) -> Generator:
        """Yield clickstream event dicts."""
        for _ in range(num_events):             # sessions will have multiple events, so we generate session-level data and then events within that session
            customer = random.choice(self.customers) if random.random() > 0.3 else None
            session_id = str(uuid.uuid4())
            events_in_session = random.randint(1, 20)
            event_ts = fake.date_time_between(start_date="-30d", end_date="now")

            for j in range(events_in_session):  # Generate multiple events for the same session, with timestamps incrementing by a few seconds to simulate user behavior within a session
                event = ClickstreamEvent.generate(
                    session_id=session_id,
                    customer_id=customer.customer_id if customer else None,
                    event_ts=event_ts + timedelta(seconds=j * random.randint(5, 120))
                )
                yield asdict(event)

    def generate_inventory_csv(self, filepath: str) -> None:
        """Write inventory snapshot as CSV."""
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "snapshot_ts", "warehouse_id", "product_sku", "quantity_on_hand",
                "quantity_reserved", "quantity_available", "reorder_point",
                "reorder_quantity", "unit_cost", "last_received_date", "supplier_id"
            ])
            for wh in WAREHOUSES:
                for product in PRODUCT_CATALOG:
                    qty_on_hand = random.randint(0, 5000)
                    qty_reserved = random.randint(0, qty_on_hand)
                    writer.writerow([
                        datetime.now(timezone.utc).isoformat(),
                        wh["id"],
                        product["sku"],
                        qty_on_hand,
                        qty_reserved,
                        qty_on_hand - qty_reserved,
                        random.randint(50, 500),    # Reorder point
                        random.randint(100, 1000),  # Reorder Quantity
                        product["cost"],
                        fake.date_between(start_date="-90d", end_date="today").isoformat(),
                        f"SUPP-{random.randint(1000, 9999)}",
                    ])
        print(f"✅ Inventory CSV written to {filepath}")

    def generate_reviews_xml(self, num_reviews: int = 10_000) -> str:
        """Generate product reviews as XML string."""
        root = ET.Element("reviews", {"generated_at": datetime.now(timezone.utc).isoformat()})

        for _ in range(num_reviews):
            customer = random.choice(self.customers)
            product = random.choice(PRODUCT_CATALOG)

            review = ET.SubElement(root, "review")
            ET.SubElement(review, "review_id").text = str(uuid.uuid4())
            ET.SubElement(review, "customer_id").text = customer.customer_id
            ET.SubElement(review, "customer_email_hash").text = customer.email_hash
            ET.SubElement(review, "product_sku").text = product["sku"]
            ET.SubElement(review, "product_name").text = product["name"]
            ET.SubElement(review, "rating").text = str(random.randint(1, 5))
            ET.SubElement(review, "title").text = fake.catch_phrase()
            ET.SubElement(review, "body").text = fake.text(max_nb_chars=500)
            ET.SubElement(review, "review_date").text = fake.date_between(
                start_date="-2y", end_date="today"
            ).isoformat()
            ET.SubElement(review, "verified_purchase").text = str(random.random() > 0.2).lower()
            ET.SubElement(review, "helpful_votes").text = str(random.randint(0, 150))
            ET.SubElement(review, "images_count").text = str(random.randint(0, 5))
            ET.SubElement(review, "sentiment").text = random.choice(
                ["positive"] * 60 + ["neutral"] * 20 + ["negative"] * 20
            )

        return (                                            
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            + ET.tostring(root, encoding="unicode")
        )

    def save_customers_json(self, filepath: str) -> None:
        """Save customer master data as JSONL (line-delimited JSON)."""
        with open(filepath, "w") as f:
            for customer in self.customers:
                f.write(json.dumps(asdict(customer)) + "\n")
        print(f"✅ Customers JSONL written to {filepath}")


# ── CLI ENTRYPOINT ────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="NexusFlow E-Commerce Data Generator")
    parser.add_argument("--mode", choices=["batch", "stream", "all"], default="batch")
    parser.add_argument("--output-dir", default="/tmp/nexusflow-data")
    parser.add_argument("--num-customers", type=int, default=10_000)
    parser.add_argument("--num-orders", type=int, default=100_000)
    parser.add_argument("--num-events", type=int, default=500_000)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    gen = EcommerceDataGenerator(num_customers=args.num_customers)

    if args.mode in ("batch", "all"):
        print("📦 Generating batch data...")

        # Customers
        gen.save_customers_json(f"{args.output_dir}/customers.jsonl")

        # Orders (write in chunks)
        orders_file = f"{args.output_dir}/orders.jsonl"
        with open(orders_file, "w") as f:
            for i, order in enumerate(gen.generate_orders_batch(args.num_orders)):
                f.write(json.dumps(order) + "\n")
                if i % 10_000 == 0:
                    print(f"  Orders: {i:,}/{args.num_orders:,}")
        print(f"✅ Orders JSONL written to {orders_file}")

        # Inventory
        gen.generate_inventory_csv(f"{args.output_dir}/inventory.csv")

        # Reviews
        reviews_xml = gen.generate_reviews_xml(10_000)
        with open(f"{args.output_dir}/reviews.xml", "w") as f:
            f.write(reviews_xml)
        print(f"✅ Reviews XML written")

    print("\n🎉 Data generation complete!")
    print(f"   Output directory: {args.output_dir}")
