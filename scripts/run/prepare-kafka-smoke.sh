#!/usr/bin/env bash
#
# Recreate the smoke Kafka topic used by SR-06 and SK-07.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$ROOT"
.venv/bin/python - <<'PY'
from confluent_kafka.admin import AdminClient
admin = AdminClient({"bootstrap.servers": "localhost:19092"})
for topic in ("orders-smoke", "orders-sink-smoke"):
    futures = admin.delete_topics([topic], operation_timeout=20)
    for name, future in futures.items():
        try:
            future.result()
            print(f"deleted topic {name}")
        except Exception as error:
            print(f"topic {name}: {error}")
PY

.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/seed/orders.jsonl \
  --format json \
  --topic orders-smoke \
  --limit 10000
