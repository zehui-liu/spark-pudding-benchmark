#!/usr/bin/env bash
#
# E2E-11 join variant: a self stream-stream join with event-time watermarks,
# a checkpoint and a restart with no new input.
#
# Usage: scripts/run/run-join-e2e.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
SUMMARY="$ROOT/results/runs/join-e2e-summary.md"
TOPIC="orders-join"
EXPECTED=5

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
echo "# Stream-stream join case ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
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

if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-kafka-1'; then
  docker compose -f "$COMPOSE_FILE" --profile streaming up -d --wait kafka >/dev/null
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
  --file "$ROOT/data/seed/e2e11_join.jsonl" \
  --format json --topic "$TOPIC" --limit 10 \
  >>"$ROOT/results/runs/join-e2e-producer.log" 2>&1 ||
  fail "join producer failed"
pass "produced 10 messages (5 PAID rows) to $TOPIC"

rm -rf "$ROOT/results/state/e2e-11-join"
"$ROOT/scripts/run/run-case.sh" E2E-11-stream-self-join --timeout 600 \
  >>"$ROOT/results/runs/join-e2e-runner.log" 2>&1 ||
  fail "self-join first run failed"
actual="$(rows "$ROOT/results/runs/E2E-11-stream-self-join/output/result")"
[[ "$actual" -eq "$EXPECTED" ]] ||
  fail "self-join wrote $actual rows, expected $EXPECTED"
pass "self-join with watermarks wrote $EXPECTED rows"

"$ROOT/scripts/run/run-case.sh" E2E-11-stream-self-join --timeout 600 \
  >>"$ROOT/results/runs/join-e2e-runner.log" 2>&1 ||
  fail "self-join restart failed"
restart_rows="$(rows "$ROOT/results/runs/E2E-11-stream-self-join/output/result")"
[[ "$restart_rows" -eq 0 ]] ||
  fail "self-join restart reprocessed $restart_rows rows"
pass "self-join restart from the checkpoint wrote no duplicate rows"

echo >> "$SUMMARY"
echo "ALL PASS: E2E-11-join (self stream-stream join and checkpoint restart)" >> "$SUMMARY"
echo
echo "ALL PASS: E2E-11-join (self stream-stream join and checkpoint restart)"
echo "summary: ${SUMMARY#$ROOT/}"
