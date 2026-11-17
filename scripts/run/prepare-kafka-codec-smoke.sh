#!/usr/bin/env bash
#
# Recreate the Avro and Protobuf Kafka topics used by E2E-10 and produce the
# first 10,000 seed orders into each with the Confluent wire format.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$ROOT"
.venv/bin/python - <<'PY'
from confluent_kafka.admin import AdminClient

admin = AdminClient({"bootstrap.servers": "localhost:19092"})
for topic in ("orders-avro", "orders-protobuf"):
    futures = admin.delete_topics([topic], operation_timeout=20)
    for name, future in futures.items():
        try:
            future.result()
            print(f"deleted topic {name}")
        except Exception as error:
            print(f"topic {name}: {error}")
PY

.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --format avro --topic orders-avro --subject orders-avro-value \
  --file data/seed/orders.jsonl --limit 10000
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --format protobuf --topic orders-protobuf --subject orders-protobuf-value \
  --file data/seed/orders.jsonl --limit 10000
