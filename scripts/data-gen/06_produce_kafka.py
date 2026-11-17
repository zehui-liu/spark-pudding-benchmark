#!/usr/bin/env python3
"""Produce generated orders to Kafka in JSON, Avro or Protobuf format.

JSON needs only confluent-kafka. Avro additionally needs fastavro. Protobuf is
encoded manually for the fixed config/schemas/orders.proto schema and uses the
Confluent wire format, so no generated Python protobuf classes are required.
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover - exercised by the operator
    print(
        "confluent-kafka is required: python3 -m pip install -r scripts/data-gen/requirements.txt",
        file=sys.stderr,
    )
    raise SystemExit(2)

try:
    import fastavro
except ImportError:  # Avro is optional; JSON still works without it.
    fastavro = None

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA = ROOT / "data" / "generated"
SCHEMA_DIR = ROOT / "config" / "schemas"

DEFAULT_TOPICS = {
    "json": "orders-json",
    "avro": "orders-avro",
    "protobuf": "orders-protobuf",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--format", choices=("json", "avro", "protobuf"), default="json")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--subject", default=None)
    parser.add_argument("--schema-registry", default="http://localhost:18081")
    parser.add_argument("--schema-file", type=Path, default=None)
    parser.add_argument("--file", type=Path, default=DEFAULT_DATA / "orders.jsonl")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--rate", type=int, default=0, help="messages per second; 0 = unlimited")
    parser.add_argument("--compression", default="none")
    return parser.parse_args()


def register_schema(registry: str, subject: str, schema_type: str, schema_text: str) -> int:
    url = f"{registry.rstrip('/')}/subjects/{urllib.parse.quote(subject, safe='')}/versions"
    body = json.dumps({"schema": schema_text, "schemaType": schema_type}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise SystemExit(f"schema registration failed ({error.code}): {detail}")
    except urllib.error.URLError as error:
        raise SystemExit(f"schema registry is not reachable at {registry}: {error}")
    return int(payload["id"])


def varint(value: int) -> bytes:
    if value < 0:
        value += 1 << 64
    out = bytearray()
    while True:
        chunk = value & 0x7F
        value >>= 7
        if value:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def tag(field: int, wire_type: int) -> bytes:
    return varint((field << 3) | wire_type)


def pb_int64(field: int, value: int) -> bytes:
    return tag(field, 0) + varint(value)


def pb_double(field: int, value: float) -> bytes:
    return tag(field, 1) + struct.pack("<d", value)


def pb_string(field: int, value: str) -> bytes:
    encoded = value.encode("utf-8")
    return tag(field, 2) + varint(len(encoded)) + encoded


def encode_protobuf(document: dict) -> bytes:
    return b"".join(
        [
            pb_int64(1, int(document["order_id"])),
            pb_int64(2, int(document["customer_id"])),
            pb_double(3, float(document["amount"])),
            pb_string(4, document["status"]),
            pb_string(5, document["city"]),
            pb_string(6, document["order_time"]),
            pb_string(7, document["updated_at"]),
            pb_string(8, document["payload"]),
        ]
    )


def confluent_wire(schema_id: int, payload: bytes, message_indexes: bytes = b"") -> bytes:
    return b"\x00" + struct.pack(">I", schema_id) + message_indexes + payload


def avro_record(document: dict) -> dict:
    return {
        "order_id": int(document["order_id"]),
        "customer_id": int(document["customer_id"]),
        "amount": float(document["amount"]),
        "status": document["status"],
        "city": document["city"],
        "order_time": document["order_time"],
        "updated_at": document["updated_at"],
        "payload": document["payload"],
    }


def avro_encode(schema: dict, document: dict) -> bytes:
    if fastavro is None:
        raise SystemExit("fastavro is required for --format avro")
    from io import BytesIO

    buffer = BytesIO()
    fastavro.schemaless_writer(buffer, schema, avro_record(document))
    return buffer.getvalue()


def main() -> int:
    args = parse_args()
    topic = args.topic or DEFAULT_TOPICS[args.format]
    subject = args.subject or f"{topic}-value"

    if not args.file.exists():
        raise SystemExit(f"missing dataset: {args.file}")

    schema_id = None
    avro_schema = None
    if args.format == "avro":
        schema_path = args.schema_file or (SCHEMA_DIR / "orders.avsc")
        avro_schema = json.loads(schema_path.read_text())
        schema_id = register_schema(
            args.schema_registry, subject, "AVRO", json.dumps(avro_schema)
        )
        print(f"registered Avro subject={subject} id={schema_id}", flush=True)
    elif args.format == "protobuf":
        schema_path = args.schema_file or (SCHEMA_DIR / "orders.proto")
        schema_id = register_schema(
            args.schema_registry, subject, "PROTOBUF", schema_path.read_text()
        )
        print(f"registered Protobuf subject={subject} id={schema_id}", flush=True)

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "client.id": "spark-pudding-benchmark-data-gen",
            "compression.type": args.compression,
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 1_000_000,
            "queue.buffering.max.kbytes": 2_097_152,
        }
    )

    delivered = 0
    failed = []

    def on_delivery(error, _message):
        nonlocal delivered
        if error is not None:
            failed.append(error)
        else:
            delivered += 1

    def produce_with_backpressure(message: bytes, key: bytes) -> None:
        while True:
            try:
                producer.produce(
                    topic,
                    key=key,
                    value=message,
                    on_delivery=on_delivery,
                )
                return
            except BufferError:
                # The local queue is full. Let the producer drain before retrying.
                producer.poll(0.5)

    started = time.time()
    sent = 0
    with args.file.open("r", encoding="utf-8") as handle:
        for line in handle:
            if args.limit is not None and sent >= args.limit:
                break
            line = line.strip()
            if not line:
                continue
            if args.format == "json":
                value = line.encode("utf-8")
                key = json.loads(line)["order_id"]
            else:
                document = json.loads(line)
                key = document["order_id"]
                if args.format == "avro":
                    value = confluent_wire(schema_id, avro_encode(avro_schema, document))
                else:
                    # The root message has an empty message-index path: a single
                    # zero-length varint. A non-empty path makes Confluent try to
                    # resolve a nested message and fail.
                    value = confluent_wire(
                        schema_id, encode_protobuf(document), b"\x00"
                    )
            produce_with_backpressure(value, str(key).encode("utf-8"))
            sent += 1
            if sent % 100_000 == 0:
                producer.poll(0)
                elapsed = max(time.time() - started, 1e-9)
                print(
                    f"[{topic}] sent={sent:,} delivered={delivered:,} "
                    f"({sent / elapsed:,.0f} msg/s)",
                    flush=True,
                )
            if args.rate > 0:
                target = started + sent / args.rate
                sleep_for = target - time.time()
                if sleep_for > 0:
                    time.sleep(sleep_for)
            producer.poll(0)

    producer.flush(120)
    if failed:
        print(f"delivery failed for {len(failed)} messages; first: {failed[0]}", file=sys.stderr)
        return 1
    print(f"[{topic}] sent={sent:,} delivered={delivered:,}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
