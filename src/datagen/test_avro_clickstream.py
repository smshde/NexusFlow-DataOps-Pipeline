import io
import json

import fastavro
from kafka_producer import CLICKSTREAM_AVRO_SCHEMA


def test_clickstream_avro_roundtrip():
    schema = json.loads(CLICKSTREAM_AVRO_SCHEMA)
    parsed = fastavro.parse_schema(schema)
    event = {
        "event_id": "e1",
        "session_id": "s1",
        "customer_id": None,
        "event_type": "page_view",
        "event_ts": "2026-06-26T00:00:00",
        "page_url": "/x",
        "referrer_url": None,
        "product_sku": None,
        "product_category": None,
        "search_query": None,
        "device_type": "mobile",
        "browser": "chrome",
        "os": "ios",
        "ip_hash": "h",
        "user_agent": "ua",
        "viewport_width": 390,
        "viewport_height": 844,
        "scroll_depth_pct": None,
        "time_on_page_sec": None,
        "utm_source": None,
        "utm_medium": None,
        "_ingestion_ts": "2026-06-26T00:00:00",
        "_partition_key": "s1",
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, event)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, parsed)
    assert decoded["event_id"] == "e1"
    assert decoded["customer_id"] is None
