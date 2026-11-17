#!/usr/bin/env bash
#
# Recreate the deterministic topics used by E2E-11:
#   orders-window-smoke: 600 events over 10 minutes (cities A/B) plus one
#     advance event at +30 minutes so the 5-minute windows close.
#   orders-dedup-smoke: 1,000 unique order ids followed by 100 duplicates,
#     all with the same event time so the watermark does not evict state.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$ROOT"
.venv/bin/python - <<'PY'
import json
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

topics = ("orders-window-smoke", "orders-dedup-smoke")
admin = AdminClient({"bootstrap.servers": "localhost:19092"})
for name, future in admin.delete_topics(list(topics), operation_timeout=20).items():
    try:
        future.result()
        print(f"deleted topic {name}")
    except Exception as error:
        print(f"topic {name}: {error}")

producer = Producer({"bootstrap.servers": "localhost:19092"})

def send(topic: str, record: dict) -> None:
    producer.produce(topic, json.dumps(record, separators=(",", ":")).encode())

base = datetime(2026, 1, 1, tzinfo=timezone.utc)
for i in range(600):
    moment = base + timedelta(seconds=i)
    send("orders-window-smoke", {
        "order_id": i + 1,
        "city": "A" if i % 2 == 0 else "B",
        "amount": 1.0,
        "order_time": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
send("orders-window-smoke", {
    "order_id": 9999,
    "city": "A",
    "amount": 1.0,
    "order_time": (base + timedelta(seconds=1800)).strftime("%Y-%m-%dT%H:%M:%SZ"),
})

fixed = base.strftime("%Y-%m-%dT%H:%M:%SZ")
unique = [
    {"order_id": i + 1, "city": "A", "amount": 1.0, "order_time": fixed}
    for i in range(1000)
]
for record in unique + unique[:100]:
    send("orders-dedup-smoke", record)

producer.flush(30)
print("produced window=601 dedup=1100")
PY
