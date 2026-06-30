from kafka_consumer import decode_message


def test_decode_json_topic_passthrough():
    raw = b'{"event_id": "e1"}'
    out = decode_message("orders", raw, deserializer=None)
    assert out["event_id"] == "e1"


def test_decode_clickstream_uses_deserializer():
    class FakeDeser:
        def deserialize(self, topic, data):
            return ({"event_id": "e2"}, None)

    out = decode_message("clickstream", b"avro-bytes", deserializer=FakeDeser())
    assert out["event_id"] == "e2"
