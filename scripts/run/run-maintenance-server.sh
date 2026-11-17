#!/usr/bin/env bash
#
# OS-07: HTTP maintenance control plane.
#
# Starts `pudding maintenance-server` on a local port and verifies:
#   - /healthz is anonymous; other endpoints require the bearer token;
#   - submission, idempotency keys, task polling and cancellation;
#   - destructive requests require `confirmed`;
#   - the durable state and audit files are written without credentials.
#
# Usage: scripts/run/run-maintenance-server.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STATE_DIR="$ROOT/results/state/os-07"
SUMMARY="$ROOT/results/runs/maintenance-server-summary.md"
PORT="${OS07_PORT:-18087}"
TOKEN_FILE="$STATE_DIR/token"
STATE_FILE="$STATE_DIR/tasks.json"
AUDIT_FILE="$STATE_DIR/audit.jsonl"
SERVER_LOG="$STATE_DIR/server.log"
RESP="$STATE_DIR/response.json"

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
export SPARK_HOME PUDDING_HOME SPARK_MASTER="${SPARK_MASTER:-local[2]}"

mkdir -p "$STATE_DIR" "$ROOT/results/runs"
rm -f "$STATE_FILE" "$AUDIT_FILE"
: > "$SUMMARY"
echo "# Maintenance-server cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

server_pid=""
stop_server() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    # spark-submit may exec the driver JVM; kill the whole process group so
    # the single-threaded task worker cannot outlive the test.
    kill -TERM -- "-$server_pid" 2>/dev/null || kill -TERM "$server_pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$server_pid" 2>/dev/null || break
      sleep 1
    done
    kill -KILL -- "-$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap stop_server EXIT

umask 077
cat > "$STATE_DIR/iceberg.properties" <<EOF
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=hadoop
spark.sql.catalog.lake.warehouse=file://$ROOT/data/generated/lakehouse/iceberg
EOF

# OS-07 reuses the small SK-11 table for the read-only status tasks.
"$ROOT/scripts/run/run-case.sh" SK-11 --timeout 900 \
  >>"$ROOT/results/runs/maintenance-server-runner.log" 2>&1 ||
  fail "could not rebuild the Iceberg table for the server tasks"

TOKEN="$(openssl rand -hex 16)"
printf '%s' "$TOKEN" > "$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

echo "== starting the maintenance server on 127.0.0.1:$PORT =="
set -m
env -u HADOOP_CONF_DIR PUDDING_MAINTENANCE_TOKEN="$TOKEN" \
  "$PUDDING_HOME/bin/pudding" maintenance-server \
  --host 127.0.0.1 --port "$PORT" \
  --state-file "$STATE_FILE" --audit-file "$AUDIT_FILE" \
  --conf-file "$STATE_DIR/iceberg.properties" \
  --master 'local[2]' >"$SERVER_LOG" 2>&1 &
server_pid=$!
set +m

for _ in $(seq 1 180); do
  if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 ||
  fail "maintenance server did not become healthy"
pass "OS-07 maintenance server started; /healthz is anonymous"

code="$(curl -sS -o "$RESP" -w '%{http_code}' "http://127.0.0.1:$PORT/v1/capabilities")"
[[ "$code" == "401" ]] || fail "/v1/capabilities without a token returned $code"
grep -q "unauthorized" "$RESP" || fail "unauthenticated response did not say unauthorized"
pass "OS-07 rejects unauthenticated maintenance requests"

code="$(curl -sS -o "$RESP" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$PORT/v1/capabilities")"
[[ "$code" == "200" ]] || fail "/v1/capabilities with a token returned $code"
python3 - "$RESP" <<'PY' || fail "capabilities response is incomplete"
import json
import sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
formats = value.get("formats", value)
for name in ("iceberg", "paimon", "hudi"):
    if name not in formats:
        raise SystemExit(f"missing format {name}: {value}")
PY
pass "OS-07 /v1/capabilities lists iceberg, paimon and hudi"

status_body='{"format":"iceberg","action":"status","catalog":"lake","table":"pudding_test.sk11_orders"}'
code="$(curl -sS -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: os07-status-1' -d "$status_body" \
  "http://127.0.0.1:$PORT/v1/maintenance")"
[[ "$code" == "202" ]] || fail "status submission returned $code"
first_id="$(python3 - "$RESP" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["id"])
PY
)"

code="$(curl -sS -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: os07-status-1' -d "$status_body" \
  "http://127.0.0.1:$PORT/v1/maintenance")"
[[ "$code" == "202" ]] || fail "idempotent resubmission returned $code"
repeat_id="$(python3 - "$RESP" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["id"])
PY
)"
[[ "$first_id" == "$repeat_id" ]] || fail "idempotency key created a second task"
pass "OS-07 idempotency key returns the original task"

# The first status task occupies the single worker while the second waits.
code="$(curl -sS -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: os07-queued-1' -d "$status_body" \
  "http://127.0.0.1:$PORT/v1/maintenance")"
[[ "$code" == "202" ]] || fail "queued submission returned $code"
queued_id="$(python3 - "$RESP" <<'PY'
import json
import sys
print(json.load(open(sys.argv[1], encoding="utf-8"))["id"])
PY
)"

code="$(curl -sS -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$PORT/v1/maintenance/$queued_id/cancel")"
[[ "$code" == "202" ]] || fail "cancel of a queued task returned $code"
python3 - "$RESP" <<'PY' || fail "queued task was not cancelled"
import json
import sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
if value["status"] != "cancelled":
    raise SystemExit(value)
PY
pass "OS-07 cancels a queued task"

for _ in $(seq 1 240); do
  task_status="$(curl -sS -H "Authorization: Bearer $TOKEN" \
    "http://127.0.0.1:$PORT/v1/maintenance/$first_id" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  case "$task_status" in
    succeeded) break ;;
    failed|cancelled) fail "status task finished as $task_status" ;;
  esac
  sleep 1
done
[[ "$task_status" == "succeeded" ]] || fail "status task did not finish"
pass "OS-07 task polling reports succeeded"

code="$(curl -sS -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$PORT/v1/maintenance/$first_id/cancel")"
[[ "$code" == "409" ]] || fail "cancel of a finished task returned $code"
grep -q "already finished" "$RESP" || fail "finished-task cancel message is missing"
pass "OS-07 refuses to cancel a finished task"

code="$(curl -sS -o "$RESP" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:$PORT/v1/maintenance?limit=10")"
[[ "$code" == "200" ]] || fail "task list returned $code"
python3 - "$RESP" "$first_id" "$queued_id" <<'PY' || fail "task list is incomplete"
import json
import sys
tasks = json.load(open(sys.argv[1], encoding="utf-8"))
ids = {task["id"] for task in tasks}
for expected in sys.argv[2:]:
    if expected not in ids:
        raise SystemExit(f"missing {expected}: {ids}")
PY
pass "OS-07 lists submitted tasks"

code="$(curl -sS -o "$RESP" -w '%{http_code}' -X POST \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"format":"iceberg","action":"expire-snapshots","catalog":"lake","table":"pudding_test.sk11_orders","retention":{"retainLast":1}}' \
  "http://127.0.0.1:$PORT/v1/maintenance")"
[[ "$code" == "400" ]] || fail "unconfirmed destructive request returned $code"
grep -q "add --confirm or --dry-run" "$RESP" || fail "confirmation guard message is missing"
pass "OS-07 rejects an unconfirmed destructive request"

[[ -s "$AUDIT_FILE" ]] || fail "audit file is empty"
for event in submitted cancelled succeeded; do
  grep -q "\"event\":\"$event\"" "$AUDIT_FILE" || fail "audit file is missing '$event'"
done
if grep -qF "$TOKEN" "$AUDIT_FILE" "$STATE_FILE" "$SERVER_LOG"; then
  fail "the bearer token leaked into state, audit or server logs"
fi
pass "OS-07 audit records submitted/cancelled/succeeded without the token"

stop_server
server_pid=""
pass "OS-07 maintenance server stopped cleanly"

echo >> "$SUMMARY"
echo "ALL PASS: OS-07 (maintenance-server token, idempotency, cancel, audit)" >> "$SUMMARY"
echo
echo "ALL PASS: OS-07 (maintenance-server token, idempotency, cancel, audit)"
echo "summary: ${SUMMARY#$ROOT/}"
