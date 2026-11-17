#!/usr/bin/env bash
#
# Failure and recovery cases that need to stop services or race two jobs.
#
# Usage:
#   scripts/run/run-recovery.sh RG-05   # MySQL restart during binlog CDC
#
# The caller starts the relational profile first.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CASE_ID="${1:-}"
if [[ -z "$CASE_ID" ]]; then
  echo "usage: $0 <RG-case-id>" >&2
  exit 2
fi

COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
COMPOSE_ENV="$ROOT/config/docker/.env"
if [[ -f "$COMPOSE_ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$COMPOSE_ENV"
  set +a
fi
# Local state paths must not resolve through a cluster fs.defaultFS.
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
export PUDDING_HOME SPARK_HOME

fail() {
  echo "ASSERT FAIL: $*" >&2
  exit 1
}

mysql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T mysql \
    mysql -upudding -ppudding source_db -N -B -e "$1" 2>/dev/null
}

psql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U pudding -d source_db -v ON_ERROR_STOP=1 -At -c "$1"
}

psql_target() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U pudding -d target_db -v ON_ERROR_STOP=1 -At -c "$1"
}

restart_mysql() {
  docker compose -f "$COMPOSE_FILE" --profile relational stop mysql >/dev/null
  sleep 3
  docker compose -f "$COMPOSE_FILE" --profile relational start mysql >/dev/null
  local i
  for i in $(seq 1 60); do
    if docker inspect -f '{{.State.Health.Status}}' pudding-test-mysql-1 \
        2>/dev/null | grep -q healthy; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_cdc() {
  local log="$1"
  local i
  for i in $(seq 1 180); do
    if grep -q "started mysql CDC task" "$log" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

insert_order() {
  local marker="$1"
  local order_id
  order_id="$(mysql_source "SELECT COALESCE(MAX(order_id), 0) + 1 FROM orders;")"
  mysql_source "
    INSERT INTO orders (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES ($order_id, 1, 12.34, 'PAID', 'shanghai', now(), now(), '$marker');
  "
}

rg_05() {
  local case_dir="$ROOT/results/runs/SR-11"
  local log="$case_dir/submit.log"
  local before="rg05-before-restart"
  local after="rg05-after-restart"

  "$ROOT/scripts/run/run-case.sh" SR-11 --timeout 240 --allow-timeout &
  local submit_pid=$!
  if ! wait_for_cdc "$log"; then
    wait "$submit_pid" || true
    fail "CDC task did not start"
  fi
  # "started" is logged when task.start() returns; give the binlog reader a
  # moment to establish its position so the first insert is not skipped.
  sleep 10

  # Emit one row before the restart so Debezium has a saved binlog offset. The
  # reconnect then resumes from a known position instead of doing a fresh
  # schema_only start at whatever point the database happens to be at.
  insert_order "$before"
  local i
  for i in $(seq 1 30); do
    if grep -rq "$before" "$case_dir/output" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if ! grep -rq "$before" "$case_dir/output" 2>/dev/null; then
    wait "$submit_pid" || true
    fail "pre-restart row '$before' was not captured"
  fi

  echo "== restarting MySQL while the CDC job is running =="
  restart_mysql || {
    wait "$submit_pid" || true
    fail "MySQL did not become healthy after restart"
  }
  insert_order "$after"

  local code=0
  wait "$submit_pid" || code=$?
  mkdir -p "$ROOT/results/runs/RG-05"
  cp "$log" "$ROOT/results/runs/RG-05/submit.log"

  if [[ "$code" -ne 0 ]]; then
    fail "CDC job exited $code after the MySQL restart; see results/runs/RG-05/submit.log"
  fi
  if ! grep -rq "$after" "$case_dir/output" 2>/dev/null; then
    fail "post-restart row '$after' was not captured"
  fi

  echo "RG-05 PASS (CDC resumed after a MySQL restart and captured the new row)"
}

rg_02() {
  local run_dir="$ROOT/results/runs/RG-02"
  local job="$run_dir/job.yaml"
  local env_file="$run_dir/env.properties"

  psql_source "
    DROP TABLE IF EXISTS orders_lock;
    CREATE TABLE orders_lock (LIKE orders INCLUDING ALL);
    INSERT INTO orders_lock SELECT * FROM orders WHERE order_id <= 200000;
  "
  psql_target "
    DROP TABLE IF EXISTS rg02_orders;
    CREATE TABLE rg02_orders (LIKE e2e04_orders INCLUDING ALL);
  "
  rm -rf "$ROOT/results/state/rg-02" "$run_dir"
  mkdir -p "$run_dir"

  export PUDDING_TEST_ROOT="$ROOT"
  export PG_URL="jdbc:postgresql://localhost:${PG_PORT:-15432}/source_db"
  export PG_TARGET_URL="jdbc:postgresql://localhost:${PG_PORT:-15432}/target_db"
  export PG_USER="${PG_USER:-pudding}"
  export PG_PASSWORD="${PG_PASSWORD:-pudding}"
  envsubst '${SPARK_HOME} ${PUDDING_HOME} ${PUDDING_TEST_ROOT} ${PG_URL} ${PG_TARGET_URL} ${PG_USER} ${PG_PASSWORD}' \
    < "$ROOT/config/jobs/RG-02-lock.yaml" > "$job"
  {
    echo "JAVA_HOME=$JAVA_HOME"
    echo "SPARK_HOME=$SPARK_HOME"
  } > "$env_file"

  echo "== job A acquires the incremental state lock =="
  "$PUDDING_HOME/bin/pudding" submit "$env_file" "$job" > "$run_dir/a.log" 2>&1 &
  local pid_a=$!
  local i
  for i in $(seq 1 180); do
    if grep -q "acquired incremental state lock" "$run_dir/a.log" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if ! grep -q "acquired incremental state lock" "$run_dir/a.log" 2>/dev/null; then
    wait "$pid_a" || true
    fail "job A did not acquire the lock"
  fi

  echo "== job B must be rejected by the same lock =="
  local code_b=0
  "$PUDDING_HOME/bin/pudding" submit "$env_file" "$job" > "$run_dir/b.log" 2>&1 || code_b=$?
  if [[ "$code_b" -eq 0 ]]; then
    wait "$pid_a" || true
    fail "job B acquired the lock while job A was running"
  fi
  if ! grep -qi "lock" "$run_dir/b.log"; then
    wait "$pid_a" || true
    fail "job B failed ($code_b) without a lock message"
  fi

  local code_a=0
  wait "$pid_a" || code_a=$?
  [[ "$code_a" -eq 0 ]] || fail "job A exited $code_a"

  local rows
  rows="$(psql_target "SELECT count(*) FROM rg02_orders;")"
  [[ "$rows" -eq 200000 ]] || fail "target rows $rows != 200000"

  echo "RG-02 PASS (one writer, second job rejected by the lock, 200,000 rows)"
}

rg_07() {
  local pudding="$PUDDING_HOME/bin/pudding"
  local state="$ROOT/results/state/e2e-04"
  [[ -d "$state" ]] || fail "state target $state is missing; run E2E-04 first"

  local status_json
  status_json="$("$pudding" state status "$state" orders_source --json 2>/dev/null)"
  local cursor
  cursor="$(python3 - "$status_json" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
if data.get("sourceId") != "orders_source":
    raise SystemExit(f"unexpected sourceId: {data.get('sourceId')}")
if not data.get("committedCursor"):
    raise SystemExit("missing committedCursor")
if data.get("pending") is not None:
    raise SystemExit("unexpected pending transaction")
print(data["committedCursor"])
PY
)" || fail "state status --json did not match the expected shape"

  "$pudding" state reset "$state" orders_source \
    --cursor "2000-01-01 00:00:00" --force >/dev/null
  "$pudding" state show "$state" orders_source 2>/dev/null |
    grep -Fq "cursor: 2000-01-01 00:00:00" || fail "reset did not change the cursor"

  "$pudding" state reset "$state" orders_source \
    --cursor "$cursor" --force >/dev/null
  "$pudding" state show "$state" orders_source 2>/dev/null |
    grep -Fq "cursor: $cursor" || fail "restore did not set the original cursor"

  "$pudding" state unlock "$state" orders_source 2>/dev/null |
    grep -Fq "no incremental state lock found" || fail "unlock did not report a missing lock"

  local code=0
  local migrate
  migrate="$("$pudding" state migrate "$state" 2>&1)" || code=$?
  [[ "$code" -ne 0 ]] || fail "migrate accepted a filesystem state target"
  echo "$migrate" | grep -Fq "migrate requires a jdbc state target" ||
    fail "migrate did not name the JDBC requirement"

  "$pudding" state list "$state" 2>/dev/null |
    grep -Fq "source: orders_source" || fail "state list did not show the source"

  echo "RG-07 PASS (status JSON, reset, restore, unlock, migrate guard, list)"
}

restart_kafka() {
  docker compose -f "$COMPOSE_FILE" --profile streaming stop kafka >/dev/null
  sleep 3
  docker compose -f "$COMPOSE_FILE" --profile streaming start kafka >/dev/null
  local i
  for i in $(seq 1 60); do
    if docker inspect -f '{{.State.Health.Status}}' pudding-test-kafka-1 \
        2>/dev/null | grep -q healthy; then
      return 0
    fi
    sleep 1
  done
  return 1
}

rg_04() {
  local runner="$ROOT/scripts/run/run-case.sh"
  local result="$ROOT/results/runs/RG-04/output/result"
  local delta="$ROOT/results/state/rg-04/delta.jsonl"
  local i

  "$ROOT/scripts/run/prepare-kafka-smoke.sh"
  rm -rf "$ROOT/results/state/rg-04"
  mkdir -p "$ROOT/results/state/rg-04"

  echo "== start the Kafka streaming job =="
  "$runner" RG-04 --timeout 240 --allow-timeout &
  local runner_pid=$!
  local rows=0
  for i in $(seq 1 120); do
    rows="$(python3 - "$result" <<'PY'
import glob
import sys

print(sum(sum(1 for _ in open(path)) for path in glob.glob(sys.argv[1] + "/part-*.json")))
PY
)"
    [[ "$rows" -gt 0 ]] && break
    sleep 1
  done
  if [[ "$rows" -le 0 ]]; then
    wait "$runner_pid" || true
    fail "Kafka job produced no rows before the broker restart"
  fi

  echo "== restart Kafka and produce 2 new messages =="
  restart_kafka || {
    wait "$runner_pid" || true
    fail "Kafka did not become healthy after restart"
  }
  cat > "$delta" <<'JSONL'
{"order_id":20000001,"customer_id":1,"amount":10.10,"status":"NEW","city":"shanghai","order_time":"2026-02-01T00:00:00Z","updated_at":"2026-02-01T00:00:00Z","payload":"rg04-insert-1"}
{"order_id":20000002,"customer_id":2,"amount":20.20,"status":"NEW","city":"beijing","order_time":"2026-02-01T00:00:01Z","updated_at":"2026-02-01T00:00:01Z","payload":"rg04-insert-2"}
JSONL
  "$ROOT/.venv/bin/python" "$ROOT/scripts/run/produce-kafka-json.py" \
    --bootstrap "localhost:${KAFKA_PORT:-19092}" --topic orders-smoke --file "$delta"

  local code=0
  wait "$runner_pid" || code=$?
  if [[ "$code" -ne 0 ]]; then
    fail "Kafka job exited $code after the broker restart"
  fi

  python3 - "$result" <<'PY' || fail "Kafka round trip lost or duplicated rows"
import glob
import json
import sys

ids = []
for path in glob.glob(sys.argv[1] + "/part-*.json"):
    with open(path, "r", encoding="utf-8") as handle:
        ids.extend(int(json.loads(line)["order_id"]) for line in handle)
if len(ids) != 10002 or len(set(ids)) != 10002:
    raise SystemExit(f"rows={len(ids)} unique={len(set(ids))}, expected 10002/10002")
PY

  echo "RG-04 PASS (Kafka broker restart, checkpoint resume, 10,002 unique rows)"
}

rg_01() {
  local pudding="$PUDDING_HOME/bin/pudding"
  local runner="$ROOT/scripts/run/run-case.sh"
  local state="$ROOT/results/state/rg-01"
  local log="$ROOT/results/runs/RG-01/submit.log"
  local i

  "$ROOT/scripts/run/prepare-postgres-smoke.sh"
  psql_target "TRUNCATE rg01_orders;"
  rm -rf "$state"

  echo "== start the job and kill the driver after the state lock =="
  "$runner" RG-01 --timeout 300 \
    > "$ROOT/results/runs/RG-01-runner.log" 2>&1 &
  local runner_pid=$!
  for i in $(seq 1 180); do
    if grep -q "acquired incremental state lock" "$log" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if ! grep -q "acquired incremental state lock" "$log" 2>/dev/null; then
    wait "$runner_pid" || true
    fail "RG-01 job did not acquire the state lock"
  fi
  sleep 3
  local driver_pid
  driver_pid="$(pgrep -f "name rg-01-kill-recovery" | head -1)"
  if [[ -z "$driver_pid" ]]; then
    wait "$runner_pid" || true
    fail "RG-01 driver process was not found"
  fi
  kill -9 "$driver_pid"
  wait "$runner_pid" || true

  local status
  status="$("$pudding" state status "$state" orders_source --json 2>/dev/null)"
  local pending="no"
  if python3 - "$status" <<'PY'
import json
import sys

pending = json.loads(sys.argv[1]).get("pending")
if pending is None:
    raise SystemExit(1)
PY
  then
    pending="yes"
  fi

  "$pudding" state unlock "$state" orders_source >/dev/null 2>&1 || true

  echo "== rerun after the kill -9 =="
  "$runner" RG-01 --timeout 600
  local rows duplicates
  rows="$(psql_target "SELECT count(*) FROM rg01_orders;")"
  [[ "$rows" -eq 10000 ]] || fail "recovery rows $rows != 10000"
  duplicates="$(psql_target "SELECT count(*) FROM (SELECT order_id FROM rg01_orders GROUP BY order_id HAVING count(*) > 1) d;")"
  [[ "$duplicates" -eq 0 ]] || fail "duplicate keys after recovery: $duplicates"
  status="$("$pudding" state status "$state" orders_source --json 2>/dev/null)"
  python3 - "$status" <<'PY' || fail "pending transaction remains after recovery"
import json
import sys

if json.loads(sys.argv[1]).get("pending") is not None:
    raise SystemExit("pending transaction remains")
PY

  echo "RG-01 PASS (driver kill -9, pending=$pending, 10,000 rows, no duplicates)"
}

rg_03() {
  local pudding="$PUDDING_HOME/bin/pudding"
  local runner="$ROOT/scripts/run/run-case.sh"
  local abort_state="$ROOT/results/state/rg-03"
  local commit_state="$ROOT/results/state/rg-03-commit"
  local status rows

  "$ROOT/scripts/run/prepare-postgres-smoke.sh"
  psql_target "TRUNCATE rg03_orders; TRUNCATE rg03_commit_orders;"
  rm -rf "$abort_state" "$commit_state"

  echo "== bad sink leaves a pending transaction =="
  RG03_STATE_PATH="$abort_state" RG03_TARGET_TABLE="public.rg03_missing" \
    "$runner" RG-03 --timeout 900 || true
  status="$("$pudding" state status "$abort_state" orders_source --json 2>/dev/null)"
  python3 - "$status" <<'PY' || fail "no pending transaction after the failed sink"
import json
import sys

pending = json.loads(sys.argv[1]).get("pending")
if pending is None or not pending.get("transactionId"):
    raise SystemExit("missing pending transaction")
PY

  echo "== state resolve --abort, then replay the range with a working sink =="
  "$pudding" state resolve "$abort_state" orders_source --abort --force >/dev/null
  status="$("$pudding" state status "$abort_state" orders_source --json 2>/dev/null)"
  python3 - "$status" <<'PY' || fail "pending transaction still present after --abort"
import json
import sys

if json.loads(sys.argv[1]).get("pending") is not None:
    raise SystemExit("pending transaction is still present")
PY
  RG03_STATE_PATH="$abort_state" RG03_TARGET_TABLE="public.rg03_orders" \
    "$runner" RG-03 --timeout 900
  rows="$(psql_target "SELECT count(*) FROM rg03_orders;")"
  [[ "$rows" -eq 10000 ]] || fail "abort replay rows $rows != 10000"

  echo "== state resolve --commit skips the unwritten batch =="
  RG03_STATE_PATH="$commit_state" RG03_TARGET_TABLE="public.rg03_commit_missing" \
    "$runner" RG-03 --timeout 900 || true
  status="$("$pudding" state status "$commit_state" orders_source --json 2>/dev/null)"
  python3 - "$status" <<'PY' || fail "no pending transaction for the commit case"
import json
import sys

pending = json.loads(sys.argv[1]).get("pending")
if pending is None or not pending.get("transactionId"):
    raise SystemExit("missing pending transaction")
PY
  "$pudding" state resolve "$commit_state" orders_source --commit --force >/dev/null
  RG03_STATE_PATH="$commit_state" RG03_TARGET_TABLE="public.rg03_commit_orders" \
    "$runner" RG-03 --timeout 900
  rows="$(psql_target "SELECT count(*) FROM rg03_commit_orders;")"
  [[ "$rows" -eq 0 ]] || fail "commit path wrote $rows rows; expected the batch to be skipped"

  echo "RG-03 PASS (pending transaction, abort replay, commit skips the batch)"
}

rg_06() {
  local runner="$ROOT/scripts/run/run-case.sh"
  local base="$ROOT/results/state/rg-06"
  rm -rf "$base"
  # A streaming file source requires a directory base path.
  mkdir -p "$base/input" "$base/scale-input"
  cp "$ROOT/data/seed/e2e03_orders.csv" "$base/input/"
  ln "$ROOT/data/generated/orders.csv" "$base/scale-input/orders.csv"

  echo "== natural available-now completion deletes its checkpoint =="
  RG06_CHECKPOINT="$base/success" RG06_INPUT="$base/input" \
    "$runner" RG-06-delete-on-success --timeout 600
  [[ ! -e "$base/success" ]] ||
    fail "checkpoint still exists after a successful finite run"
  local rows
  rows="$(python3 - "$ROOT/results/runs/RG-06-delete-on-success/output/result" <<'PY'
import glob
import sys
print(sum(sum(1 for _ in open(path, encoding="utf-8"))
          for path in glob.glob(sys.argv[1] + "/**/part-*.json", recursive=True)))
PY
)"
  [[ "$rows" -eq 10 ]] || fail "successful finite run wrote $rows rows, expected 10"

  echo "== sink timeout stops the query and keeps the checkpoint =="
  RG06_CHECKPOINT="$base/timeout" \
    RG06_INPUT="$base/scale-input" \
    RG06_TIMEOUT_MS=3000 \
    "$runner" RG-06-timeout-keep --dataset scale --timeout 900 >/dev/null 2>&1 || true
  [[ -d "$base/timeout/commits" && -d "$base/timeout/offsets" ]] ||
    fail "timed-out run did not leave a checkpoint"
  grep -Fq "did not prove that its input was drained" \
    "$ROOT/results/runs/RG-06-timeout-keep/submit.log" ||
    fail "timed-out run did not report that the checkpoint was kept"

  echo "RG-06 PASS (finite success deletes the checkpoint; timeout keeps it)"
}

case "$CASE_ID" in
  RG-01) rg_01 ;;
  RG-02) rg_02 ;;
  RG-03) rg_03 ;;
  RG-04) rg_04 ;;
  RG-05) rg_05 ;;
  RG-06) rg_06 ;;
  RG-07) rg_07 ;;
  *)
    echo "no recovery script for $CASE_ID" >&2
    exit 2
    ;;
esac
