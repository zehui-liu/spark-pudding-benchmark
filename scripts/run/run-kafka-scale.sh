#!/usr/bin/env bash
#
# E2E-09 scale: 10,000,000 Kafka JSON messages into Hudi.
#
# Starts Kafka, recreates the `orders-scale` topic, produces the 10M-row
# orders.jsonl dataset, runs the streaming bulk-insert job and verifies the
# Hudi row count.
#
# Usage: scripts/run/run-kafka-scale.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
SUMMARY="$ROOT/results/runs/kafka-scale-summary.md"
TOPIC="orders-scale"
LIMIT="${KAFKA_SCALE_LIMIT:-10000000}"

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
echo "# Kafka scale case ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-kafka-1'; then
  docker compose -f "$COMPOSE_FILE" --profile streaming up -d --wait kafka \
    >/dev/null
fi

echo "== recreating topic $TOPIC =="
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
    [NewTopic(topic, num_partitions=4, replication_factor=1)],
    operation_timeout=30).items():
    future.result()
    print(f"created topic {name}")
PY

echo "== producing $LIMIT JSON messages =="
"$ROOT/.venv/bin/python" "$ROOT/scripts/data-gen/06_produce_kafka.py" \
  --file "$ROOT/data/generated/orders.jsonl" \
  --format json \
  --topic "$TOPIC" \
  --limit "$LIMIT" \
  --compression lz4 >>"$ROOT/results/runs/kafka-scale-producer.log" 2>&1 ||
  fail "Kafka producer failed"
pass "produced $LIMIT messages to $TOPIC"

rm -rf "$ROOT/results/state/e2e-09-scale"
echo "== streaming Kafka to Hudi =="
"$ROOT/scripts/run/run-case.sh" E2E-09-kafka-to-hudi-scale \
  --timeout 3600 --allow-timeout \
  >>"$ROOT/results/runs/kafka-scale-runner.log" 2>&1 ||
  fail "Kafka to Hudi scale job failed"

echo "== verifying Hudi row count =="
env -u HADOOP_CONF_DIR "$ROOT/scripts/verify/table-check.sh" hudi \
  "$ROOT/results/state/e2e-09-scale/hudi/orders" --count "$LIMIT" \
  >>"$ROOT/results/runs/kafka-scale-verify.log" 2>&1 ||
  fail "Hudi row count did not reach $LIMIT"
pass "Hudi table contains $LIMIT rows"

echo >> "$SUMMARY"
echo "ALL PASS: E2E-09-scale ($LIMIT Kafka messages into Hudi)" >> "$SUMMARY"
echo
echo "ALL PASS: E2E-09-scale ($LIMIT Kafka messages into Hudi)"
echo "summary: ${SUMMARY#$ROOT/}"
