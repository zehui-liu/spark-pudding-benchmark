#!/usr/bin/env bash
#
# Schema Registry subject and error-path coverage:
#   - Avro messages registered under a non-default subject decode successfully;
#   - a missing subject is not an error for Confluent wire-format messages because
#     the embedded schema id is authoritative (characterization test);
#   - an unknown schema id fails with a clear schema-registry error.
#
# Usage: scripts/run/run-schema-registry-subject.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
SUMMARY="$ROOT/results/runs/schema-registry-subject-summary.md"
TOPIC="orders-avro-custom"
SUBJECT="orders-avro-custom-subject"
LIMIT=1000

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
if [[ "${PUDDING_USE_HADOOP_CONF:-0}" != "1" ]]; then
  unset HADOOP_CONF_DIR
fi

SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi
export SPARK_HOME PUDDING_HOME

mkdir -p "$ROOT/results/runs"
: > "$SUMMARY"
echo "# Schema Registry subject cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

rows() {
  python3 - "$1" <<'PY'
import glob
import sys
print(sum(sum(1 for _ in open(path, encoding="utf-8"))
          for path in glob.glob(sys.argv[1] + "/**/part-*.json", recursive=True)))
PY
}

if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-schema-registry-1'; then
  docker compose -f "$COMPOSE_FILE" --profile streaming up -d --wait kafka schema-registry \
    >/dev/null
fi

"$ROOT/.venv/bin/python" - "$TOPIC" <<'PY'
import sys
from confluent_kafka.admin import AdminClient, NewTopic

topic = sys.argv[1]
admin = AdminClient({"bootstrap.servers": "localhost:19092"})
for name, future in admin.delete_topics([topic], operation_timeout=20).items():
    try:
        future.result()
    except Exception:
        pass
for name, future in admin.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)],
    operation_timeout=30).items():
    future.result()
    print(f"created topic {name}")
PY

"$ROOT/.venv/bin/python" "$ROOT/scripts/data-gen/06_produce_kafka.py" \
  --format avro --topic "$TOPIC" --subject "$SUBJECT" \
  --file "$ROOT/data/seed/orders.jsonl" --limit "$LIMIT" \
  >>"$ROOT/results/runs/schema-registry-producer.log" 2>&1 ||
  fail "Avro producer failed"
pass "produced $LIMIT Avro messages under subject $SUBJECT"

rm -rf "$ROOT/results/state/e2e-10-custom"
AVRO_TOPIC="$TOPIC" AVRO_SUBJECT="$SUBJECT" \
  "$ROOT/scripts/run/run-case.sh" E2E-10-avro-custom-subject --timeout 600 \
  >>"$ROOT/results/runs/schema-registry-runner.log" 2>&1 ||
  fail "custom-subject decode failed"
actual="$(rows "$ROOT/results/runs/E2E-10-avro-custom-subject/output/result")"
[[ "$actual" -eq "$LIMIT" ]] ||
  fail "custom-subject decode wrote $actual rows, expected $LIMIT"
pass "custom subject decoded $LIMIT Avro rows"

# Confluent wire format carries the schema id; a subject is only a lookup hint for
# payloads without an id, so a missing subject must not change the result here.
rm -rf "$ROOT/results/state/e2e-10-custom/missing-subject-checkpoint"
AVRO_TOPIC="$TOPIC" AVRO_SUBJECT="missing-subject-os10" \
  AVRO_CHECKPOINT="$ROOT/results/state/e2e-10-custom/missing-subject-checkpoint" \
  "$ROOT/scripts/run/run-case.sh" E2E-10-avro-custom-subject --timeout 600 \
  >>"$ROOT/results/runs/schema-registry-runner.log" 2>&1 ||
  fail "missing subject unexpectedly failed on an id-based message"
actual="$(rows "$ROOT/results/runs/E2E-10-avro-custom-subject/output/result")"
[[ "$actual" -eq "$LIMIT" ]] ||
  fail "missing-subject characterization wrote $actual rows, expected $LIMIT"
pass "wire-format schema id is authoritative; a missing subject does not block decoding"

# An unknown schema id is a real error path.
"$ROOT/.venv/bin/python" - <<'PY'
import struct
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:19092"})
for name, future in admin.delete_topics(["orders-avro-bad-id"], operation_timeout=20).items():
    try:
        future.result()
    except Exception:
        pass
for name, future in admin.create_topics(
    [NewTopic("orders-avro-bad-id", num_partitions=1, replication_factor=1)],
    operation_timeout=30).items():
    future.result()
producer = Producer({"bootstrap.servers": "localhost:19092"})
producer.produce(
    "orders-avro-bad-id",
    key=b"1",
    value=b"\x00" + struct.pack(">I", 999999) + b"\x00")
producer.flush()
print("sent unknown schema id 999999")
PY

rm -rf "$ROOT/results/state/e2e-10-custom/bad-id-checkpoint"
set +e
AVRO_TOPIC="orders-avro-bad-id" AVRO_SUBJECT="$SUBJECT" \
  AVRO_CHECKPOINT="$ROOT/results/state/e2e-10-custom/bad-id-checkpoint" \
  "$ROOT/scripts/run/run-case.sh" E2E-10-avro-custom-subject --timeout 300 \
  >>"$ROOT/results/runs/schema-registry-runner.log" 2>&1
code=$?
set -e
[[ "$code" -ne 0 ]] || fail "unknown schema id unexpectedly succeeded"
log="$ROOT/results/runs/E2E-10-avro-custom-subject/submit.log"
grep -q "schema registry avro decode failed for topic 'orders-avro-bad-id'" "$log" ||
  fail "unknown schema id did not report the decode failure"
grep -q "id 999999" "$log" || fail "unknown schema id did not report the schema id"
pass "unknown schema id failed with a clear error"

echo >> "$SUMMARY"
echo "ALL PASS: Schema Registry custom subject and error path" >> "$SUMMARY"
echo
echo "ALL PASS: Schema Registry custom subject and error path"
echo "summary: ${SUMMARY#$ROOT/}"
