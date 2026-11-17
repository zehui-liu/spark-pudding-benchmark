#!/usr/bin/env bash
#
# Run a multi-step end-to-end case. Component cases use run-case.sh; E2E cases
# that need target mutation, restart or replay are orchestrated here.
#
# Usage:
#   scripts/run/run-e2e.sh E2E-01
#   scripts/run/run-e2e.sh E2E-02
#   scripts/run/run-e2e.sh E2E-03
#   scripts/run/run-e2e.sh E2E-04
#   scripts/run/run-e2e.sh E2E-04-scale
#   scripts/run/run-e2e.sh E2E-05
#   scripts/run/run-e2e.sh E2E-06
#   scripts/run/run-e2e.sh E2E-06-scale
#   scripts/run/run-e2e.sh E2E-07
#   scripts/run/run-e2e.sh E2E-08
#   scripts/run/run-e2e.sh E2E-09
#   scripts/run/run-e2e.sh E2E-10
#   scripts/run/run-e2e.sh E2E-11
#   scripts/run/run-e2e.sh E2E-12
#   scripts/run/run-e2e.sh E2E-13
#   scripts/run/run-e2e.sh E2E-14
#
# The caller starts the Docker profiles the case needs. E2E-01/02/03/12/14 use no
# Docker profile; E2E-04/05/06/07/08 use relational; E2E-09/10/11 use streaming;
# E2E-13 uses relational + streaming + search.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CASE_ID="${1:-}"

if [[ -z "$CASE_ID" ]]; then
  echo "usage: $0 <E2E-case-id>" >&2
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
KAFKA_BOOTSTRAP="localhost:${KAFKA_PORT:-19092}"
# Keep local state/checkpoint paths on the local filesystem unless an HDFS case
# explicitly opts in; the shell's Hadoop config may point fs.defaultFS at HDFS.
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
PUDDING="$PUDDING_HOME/bin/pudding"
CASE_DIR="$ROOT/results/runs/$CASE_ID"

export PUDDING_HOME SPARK_HOME

psql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U pudding -d source_db -v ON_ERROR_STOP=1 -At -c "$1"
}

psql_target() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U pudding -d target_db -v ON_ERROR_STOP=1 -At -c "$1"
}

fail() {
  echo "ASSERT FAIL: $*" >&2
  exit 1
}

json_rows() {
  python3 - "$1" <<'PY'
import glob, sys
files = glob.glob(sys.argv[1] + "/**/part-*.json", recursive=True)
print(sum(sum(1 for _ in open(f, encoding="utf-8")) for f in files))
PY
}

run_case() {
  local case_id="$1"
  shift
  "$ROOT/scripts/run/run-case.sh" "$case_id" "$@"
}

e2e_01() {
  local dataset="${E2E01_DATASET:-smoke}"
  run_case E2E-01 --dataset "$dataset" --timeout 1800
  local rows
  rows="$(json_rows "$CASE_DIR/output/result")"
  [[ "$rows" -eq 10 ]] || fail "E2E-01 output rows $rows != 10"
  echo "E2E-01 PASS ($dataset dataset, 10 aggregate rows)"
}

e2e_04_scale() {
  psql_target "
    DROP TABLE IF EXISTS e2e04_scale_orders;
    CREATE TABLE e2e04_scale_orders (
      order_id    bigint PRIMARY KEY,
      customer_id bigint NOT NULL,
      amount      numeric(12, 2) NOT NULL,
      status      varchar(16) NOT NULL,
      city        varchar(32) NOT NULL,
      order_time  timestamp NOT NULL,
      updated_at  timestamp NOT NULL,
      payload     varchar(256) NOT NULL
    );
  "
  rm -rf "$ROOT/results/state/e2e-04-scale"

  echo "== scale run 1: initial full, 10,000,000 rows =="
  run_case E2E-04-scale --timeout 3600
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-1-initial.log"
  local count
  count="$(psql_target "SELECT count(*) FROM e2e04_scale_orders;")"
  [[ "$count" -eq 10000000 ]] || fail "scale target rows $count != 10000000"

  echo "== mutate source: 2 inserts + 1 update =="
  psql_source "
    INSERT INTO orders (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES
      (20000001, 1, 10.10, 'NEW', 'shanghai', now(), now(), 'e2e04scale-insert-1'),
      (20000002, 2, 20.20, 'NEW', 'beijing', now(), now(), 'e2e04scale-insert-2')
    ON CONFLICT (order_id) DO NOTHING;
    UPDATE orders
       SET amount = 999.99, payload = 'e2e04scale-updated', updated_at = now()
     WHERE order_id = 1;
  "

  echo "== scale run 2: incremental =="
  run_case E2E-04-scale --timeout 1800
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-2-incremental.log"
  count="$(psql_target "SELECT count(*) FROM e2e04_scale_orders;")"
  [[ "$count" -eq 10000002 ]] || fail "scale incremental rows $count != 10000002"
  local updated
  updated="$(psql_target "SELECT payload FROM e2e04_scale_orders WHERE order_id = 1;")"
  [[ "$updated" == "e2e04scale-updated" ]] || fail "scale updated row payload '$updated'"
  local inserted
  inserted="$(psql_target "SELECT count(*) FROM e2e04_scale_orders WHERE order_id IN (20000001, 20000002);")"
  [[ "$inserted" -eq 2 ]] || fail "scale inserted rows $inserted != 2"

  echo "E2E-04-scale PASS (10,000,000 full + 2 inserts + 1 update)"
}

e2e_04() {
  local state_dir="$ROOT/results/state/e2e-04"
  local state_target="$state_dir"

  "$ROOT/scripts/run/prepare-postgres-smoke.sh"
  psql_target "TRUNCATE e2e04_orders;"
  rm -rf "$state_dir"

  echo "== run 1: initial full =="
  run_case E2E-04 --timeout 900
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-1-initial.log"
  local count
  count="$(psql_target "SELECT count(*) FROM e2e04_orders;")"
  [[ "$count" -eq 10000 ]] || fail "initial target rows $count != 10000"

  local cursor_after_initial
  cursor_after_initial="$("$PUDDING" state show "$state_target" orders_source \
    | sed -n 's/^  cursor: //p')"
  [[ -n "$cursor_after_initial" ]] || fail "could not read initial cursor"

  echo "== mutate source: 2 inserts + 1 update =="
  psql_source "
    INSERT INTO orders_smoke (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES
      (1000001, 1, 10.10, 'NEW', 'shanghai', now(), now(), 'e2e04-insert-1'),
      (1000002, 2, 20.20, 'NEW', 'beijing', now(), now(), 'e2e04-insert-2');
    UPDATE orders_smoke
       SET amount = 999.99, payload = 'e2e04-updated', updated_at = now()
     WHERE order_id = 1;
  "

  echo "== run 2: incremental =="
  run_case E2E-04 --timeout 900
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-2-incremental.log"
  count="$(psql_target "SELECT count(*) FROM e2e04_orders;")"
  [[ "$count" -eq 10002 ]] || fail "incremental target rows $count != 10002"
  local updated
  updated="$(psql_target "SELECT payload FROM e2e04_orders WHERE order_id = 1;")"
  [[ "$updated" == "e2e04-updated" ]] || fail "updated row payload '$updated'"
  local inserted
  inserted="$(psql_target "SELECT count(*) FROM e2e04_orders WHERE order_id IN (1000001, 1000002);")"
  [[ "$inserted" -eq 2 ]] || fail "inserted rows $inserted != 2"

  echo "== run 3: no-change incremental =="
  run_case E2E-04 --timeout 900
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-3-noop.log"
  count="$(psql_target "SELECT count(*) FROM e2e04_orders;")"
  [[ "$count" -eq 10002 ]] || fail "no-op run target rows $count != 10002"

  echo "== run 4: replay the run-2 batch =="
  "$PUDDING" state reset "$state_target" orders_source \
    --cursor "$cursor_after_initial" --force >/dev/null
  run_case E2E-04 --timeout 900
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-4-replay.log"
  count="$(psql_target "SELECT count(*) FROM e2e04_orders;")"
  [[ "$count" -eq 10002 ]] || fail "replay target rows $count != 10002"
  updated="$(psql_target "SELECT payload FROM e2e04_orders WHERE order_id = 1;")"
  [[ "$updated" == "e2e04-updated" ]] || fail "replay changed updated row: '$updated'"

  echo "E2E-04 PASS (insert/update, no-change resume, replay idempotence)"
  echo "note: JDBC cursor sources cannot observe hard deletes; delete coverage is E2E-05."
}

mysql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T mysql \
    mysql -upudding -ppudding source_db -N -B -e "$1" 2>/dev/null
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

wait_for_pg_cdc() {
  local log="$1"
  local i
  for i in $(seq 1 180); do
    if grep -q "started postgres CDC task" "$log" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

mongo_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T mongo \
    mongosh --port 27018 --quiet --eval "$1"
}

e2e_06_variant() {
  local variant="$1"
  local case_dir="$ROOT/results/runs/E2E-06-$variant"
  local target_path

  "$ROOT/scripts/run/prepare-mysql-smoke.sh"
  rm -rf "$ROOT/results/state/e2e-06"
  if [[ "$variant" == "hudi" ]]; then
    target_path="$ROOT/results/state/e2e-06/hudi/orders"
    "$ROOT/scripts/run/create-hudi-table.sh" "$target_path" e2e06_orders cdc
  else
    target_path="$ROOT/results/state/e2e-06/iceberg/pudding_test/e2e06_orders"
    "$ROOT/scripts/run/create-iceberg-table.sh" \
      "$ROOT/results/state/e2e-06/iceberg" pudding_test e2e06_orders cdc
  fi

  echo "== E2E-06-$variant: snapshot + insert/update/delete =="
  run_case "E2E-06-$variant" --timeout 180 --allow-timeout &
  local submit_pid=$!
  if ! wait_for_cdc "$case_dir/submit.log"; then
    wait "$submit_pid" || true
    fail "$variant CDC task did not start"
  fi
  mysql_source "
    INSERT INTO orders_smoke (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES
      (1000601, 1, 11.11, 'NEW', 'shanghai', now(), now(), 'e2e06-insert-1'),
      (1000602, 2, 22.22, 'NEW', 'beijing', now(), now(), 'e2e06-insert-2');
    UPDATE orders_smoke
       SET amount = 777.77, payload = 'e2e06-updated', updated_at = now()
     WHERE order_id = 1;
    DELETE FROM orders_smoke WHERE order_id = 2;
  "
  wait "$submit_pid" || fail "E2E-06-$variant run 1 failed"
  cp "$case_dir/submit.log" "$case_dir/run-1-cdc.log"

  "$ROOT/scripts/verify/table-check.sh" "$variant" "$target_path" \
    --count 10001 \
    --exists "order_id = 1 AND amount = 777.77 AND payload = 'e2e06-updated'" \
    --exists "order_id = 1000601" \
    --exists "order_id = 1000602" \
    --missing "order_id = 2"

  echo "== E2E-06-$variant: restart from checkpoint =="
  run_case "E2E-06-$variant" --timeout 90 --allow-timeout
  cp "$case_dir/submit.log" "$case_dir/run-2-restart.log"
  "$ROOT/scripts/verify/table-check.sh" "$variant" "$target_path" --count 10001
}

e2e_06() {
  e2e_06_variant hudi
  e2e_06_variant iceberg
  echo "E2E-06 PASS (MySQL CDC to Hudi and Iceberg: upsert, delete, checkpoint restart)"
}

e2e_07() {
  local table_path="$ROOT/results/state/e2e-07/paimon/pudding_test.db/e2e07_orders"

  "$ROOT/scripts/run/prepare-mongo-smoke.sh"
  rm -rf "$ROOT/results/state/e2e-07"
  "$ROOT/scripts/run/create-paimon-table.sh" \
    "$ROOT/results/state/e2e-07/paimon" pudding_test e2e07_orders mongo

  echo "== run 1: snapshot + insert/update/delete =="
  run_case E2E-07 --timeout 180 --allow-timeout &
  local submit_pid=$!
  local i
  for i in $(seq 1 180); do
    if grep -q "starting MongoDB snapshot" "$CASE_DIR/submit.log" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if ! grep -q "starting MongoDB snapshot" "$CASE_DIR/submit.log" 2>/dev/null; then
    wait "$submit_pid" || true
    fail "MongoDB CDC snapshot did not start"
  fi
  mongo_source '
    const c = db.getSiblingDB("source_db").orders_smoke;
    c.insertOne({order_id: 1000701, customer_id: 1, amount: 30.30, status: "NEW",
      city: "hangzhou", order_time: new Date().toISOString(),
      updated_at: new Date().toISOString(), payload: "e2e07-insert"});
    c.updateOne({order_id: 1}, {$set: {amount: 555.55, payload: "e2e07-updated",
      updated_at: new Date().toISOString()}});
    c.deleteOne({order_id: 2});
  '
  wait "$submit_pid" || fail "run 1 failed"
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-1-cdc.log"

  "$ROOT/scripts/verify/table-check.sh" paimon "$table_path" \
    --count 10000 \
    --exists "order_id = 1 AND amount = 555.55 AND payload = 'e2e07-updated'" \
    --exists "order_id = 1000701" \
    --missing "order_id = 2"

  echo "== run 2: resume from the change-stream token =="
  run_case E2E-07 --timeout 120 --allow-timeout &
  submit_pid=$!
  sleep 20
  mongo_source '
    const c = db.getSiblingDB("source_db").orders_smoke;
    c.insertOne({order_id: 1000702, customer_id: 2, amount: 40.40, status: "NEW",
      city: "chengdu", order_time: new Date().toISOString(),
      updated_at: new Date().toISOString(), payload: "e2e07-insert-2"});
  '
  wait "$submit_pid" || fail "run 2 failed"
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-2-resume.log"
  "$ROOT/scripts/verify/table-check.sh" paimon "$table_path" \
    --count 10001 \
    --exists "order_id = 1000702"

  echo "E2E-07 PASS (snapshot, _id delete, upsert, resume token, checkpoint restart)"
}

e2e_08() {
  "$ROOT/scripts/run/prepare-postgres-smoke.sh"
  psql_target "TRUNCATE e2e08_orders;"
  rm -rf "$ROOT/results/state/e2e-08"

  echo "== run 1: snapshot + insert/update/delete =="
  run_case E2E-08 --timeout 180 --allow-timeout &
  local submit_pid=$!
  if ! wait_for_pg_cdc "$CASE_DIR/submit.log"; then
    wait "$submit_pid" || true
    fail "PostgreSQL CDC task did not start"
  fi
  psql_source "
    INSERT INTO orders_smoke (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES
      (1000801, 1, 11.11, 'NEW', 'shanghai', now(), now(), 'e2e08-insert-1'),
      (1000802, 2, 22.22, 'NEW', 'beijing', now(), now(), 'e2e08-insert-2');
    UPDATE orders_smoke
       SET amount = 888.88, payload = 'e2e08-updated', updated_at = now()
     WHERE order_id = 1;
    DELETE FROM orders_smoke WHERE order_id = 2;
  "
  wait "$submit_pid" || fail "run 1 failed"
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-1-cdc.log"

  local count
  count="$(psql_target "SELECT count(*) FROM e2e08_orders;")"
  [[ "$count" -eq 10001 ]] || fail "target rows $count != 10001"
  local updated
  updated="$(psql_target "SELECT payload FROM e2e08_orders WHERE order_id = 1;")"
  [[ "$updated" == "e2e08-updated" ]] || fail "updated row payload '$updated'"
  local inserted
  inserted="$(psql_target "SELECT count(*) FROM e2e08_orders WHERE order_id IN (1000801, 1000802);")"
  [[ "$inserted" -eq 2 ]] || fail "inserted rows $inserted != 2"
  local deleted
  deleted="$(psql_target "SELECT count(*) FROM e2e08_orders WHERE order_id = 2;")"
  [[ "$deleted" -eq 0 ]] || fail "deleted row is still present"

  echo "== run 2: restart from checkpoint, no source changes =="
  run_case E2E-08 --timeout 90 --allow-timeout
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-2-restart.log"
  count="$(psql_target "SELECT count(*) FROM e2e08_orders;")"
  [[ "$count" -eq 10001 ]] || fail "restart rows $count != 10001"

  echo "E2E-08 PASS (snapshot, upsert, delete in one transaction, checkpoint restart)"
}

e2e_05() {
  local table_path="$ROOT/data/generated/lakehouse/paimon/pudding_test.db/e2e05_orders"
  local checkpoint="$ROOT/results/state/e2e-05/checkpoint"

  "$ROOT/scripts/run/prepare-mysql-smoke.sh"
  rm -rf "$table_path" "$checkpoint"
  "$ROOT/scripts/run/create-paimon-table.sh" \
    "$ROOT/data/generated/lakehouse/paimon" pudding_test e2e05_orders

  echo "== run 1: initial snapshot + insert/update/delete =="
  run_case E2E-05 --timeout 150 --allow-timeout &
  local submit_pid=$!
  if ! wait_for_cdc "$CASE_DIR/submit.log"; then
    wait "$submit_pid" || true
    fail "MySQL CDC task did not start"
  fi
  mysql_source "
    INSERT INTO orders_smoke (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES (1000101, 1, 30.30, 'NEW', 'hangzhou', now(), now(), 'e2e05-insert');
    UPDATE orders_smoke
       SET amount = 777.77, payload = 'e2e05-updated', updated_at = now()
     WHERE order_id = 1;
    DELETE FROM orders_smoke WHERE order_id = 2;
  "
  wait "$submit_pid" || fail "run 1 failed"
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-1-initial.log"

  "$ROOT/scripts/verify/table-check.sh" paimon "$table_path" \
    --count 10000 \
    --exists "order_id = 1 AND amount = 777.77 AND payload = 'e2e05-updated'" \
    --exists "order_id = 1000101" \
    --missing "order_id = 2"

  echo "== run 2: restart from checkpoint, no source changes =="
  run_case E2E-05 --timeout 90 --allow-timeout
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-2-restart.log"
  "$ROOT/scripts/verify/table-check.sh" paimon "$table_path" --count 10000

  echo "== run 3: restart, then one more insert =="
  run_case E2E-05 --timeout 150 --allow-timeout &
  submit_pid=$!
  if ! wait_for_cdc "$CASE_DIR/submit.log"; then
    wait "$submit_pid" || true
    fail "MySQL CDC task did not restart"
  fi
  mysql_source "
    INSERT INTO orders_smoke (order_id, customer_id, amount, status, city,
      order_time, updated_at, payload)
    VALUES (1000102, 2, 40.40, 'NEW', 'chengdu', now(), now(), 'e2e05-insert-2');
  "
  wait "$submit_pid" || fail "run 3 failed"
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-3-resume.log"
  "$ROOT/scripts/verify/table-check.sh" paimon "$table_path" \
    --count 10001 \
    --exists "order_id = 1000102"

  echo "E2E-05 PASS (snapshot, upsert, delete, checkpoint restart, resume)"
}

e2e_13() {
  local rows

  echo "== E2E-13: Hive sink -> Hive source =="
  run_case SK-08 --timeout 900
  run_case SR-07 --timeout 900
  rows="$(json_rows "$ROOT/results/runs/SR-07/output/source")"
  [[ "$rows" -eq 100000 ]] || fail "Hive round-trip rows $rows != 100000"

  echo "== E2E-13: Kafka sink -> Kafka source =="
  "$ROOT/scripts/run/prepare-kafka-smoke.sh"
  run_case SK-07 --timeout 900
  run_case E2E-13-kafka --timeout 900
  rows="$(json_rows "$ROOT/results/runs/E2E-13-kafka/output/result")"
  [[ "$rows" -eq 100000 ]] || fail "Kafka round-trip rows $rows != 100000"

  echo "== E2E-13: MongoDB sink -> MongoDB source =="
  run_case SK-05 --timeout 900
  run_case E2E-13-mongo --timeout 900
  rows="$(json_rows "$ROOT/results/runs/E2E-13-mongo/output/result")"
  [[ "$rows" -eq 100000 ]] || fail "MongoDB round-trip rows $rows != 100000"

  echo "== E2E-13: Elasticsearch sink -> Elasticsearch source =="
  if ! curl -sf --max-time 5 "http://localhost:${ES_PORT:-19200}" >/dev/null; then
    fail "Elasticsearch is not reachable; run scripts/services/up.sh search first"
  fi
  run_case SK-06 --timeout 900
  run_case E2E-13-es --timeout 900
  rows="$(json_rows "$ROOT/results/runs/E2E-13-es/output/result")"
  [[ "$rows" -eq 100000 ]] || fail "Elasticsearch round-trip rows $rows != 100000"

  echo "E2E-13 PASS (Hive, Kafka, MongoDB and Elasticsearch sink/source round trips)"
}

e2e_12a() {
  local hudi_path="$ROOT/results/state/e2e-12a/hudi/orders"
  local paimon_path="$ROOT/results/state/e2e-12a/paimon/pudding_test.db/e2e12a_orders"
  rm -rf "$ROOT/results/state/e2e-12a" "$ROOT/results/state/e2e-12a-sync"
  "$ROOT/scripts/run/create-paimon-table.sh" \
    "$ROOT/results/state/e2e-12a/paimon" pudding_test e2e12a_orders hudi

  echo "== E2E-12a: build a 1,000-row Hudi source =="
  run_case E2E-12a-build --timeout 900
  "$ROOT/scripts/verify/table-check.sh" hudi "$hudi_path" --count 1000

  echo "== E2E-12a: first sync, Hudi full snapshot -> Paimon =="
  run_case E2E-12a-sync --timeout 900
  "$ROOT/scripts/verify/table-check.sh" paimon "$paimon_path" \
    --count 1000 \
    --exists "order_id = 1 AND amount = 0.37"

  echo "== E2E-12a: append 2 rows and update 1 in Hudi =="
  run_case E2E-12a-delta --timeout 900

  echo "== E2E-12a: second sync, Hudi delta commit -> Paimon =="
  run_case E2E-12a-sync --timeout 900
  "$ROOT/scripts/verify/table-check.sh" paimon "$paimon_path" \
    --count 1002 \
    --exists "order_id = 1 AND payload = 'e2e12-updated'" \
    --exists "order_id = 1001" \
    --exists "order_id = 1002"

  echo "E2E-12a PASS (Hudi full + delta commits synced to Paimon)"
}

e2e_12b() {
  local iceberg_path="$ROOT/data/generated/lakehouse/iceberg/pudding_test/e2e12b_orders"
  local hudi_path="$ROOT/results/state/e2e-12b/hudi/orders"
  rm -rf "$iceberg_path" "$ROOT/results/state/e2e-12b" "$ROOT/results/state/e2e-12b-sync"
  "$ROOT/scripts/run/create-hudi-table.sh" "$hudi_path" e2e12b_orders hudi

  echo "== E2E-12b: build a 1,000-row Iceberg source =="
  run_case E2E-12b-build --timeout 900
  "$ROOT/scripts/verify/table-check.sh" iceberg "$iceberg_path" --count 1000

  echo "== E2E-12b: first sync, Iceberg full snapshot -> Hudi =="
  run_case E2E-12b-sync --timeout 900
  "$ROOT/scripts/verify/table-check.sh" hudi "$hudi_path" --count 1000

  echo "== E2E-12b: append 2 rows to Iceberg =="
  run_case E2E-12b-delta --timeout 900

  echo "== E2E-12b: second sync, new Iceberg snapshot -> Hudi =="
  run_case E2E-12b-sync --timeout 900
  "$ROOT/scripts/verify/table-check.sh" hudi "$hudi_path" \
    --count 1002 \
    --exists "order_id = 1001" \
    --exists "order_id = 1002"

  echo "E2E-12b PASS (Iceberg full + delta snapshots synced to Hudi)"
}

e2e_12() {
  e2e_12a
  e2e_12b
  echo "E2E-12 PASS (Hudi to Paimon and Iceberg to Hudi incremental sync)"
}

e2e_11() {
  "$ROOT/scripts/run/prepare-kafka-streaming-smoke.sh"

  echo "== E2E-11-window: 5-minute event-time windows with a 1-minute watermark =="
  run_case E2E-11-window --timeout 90 --allow-timeout
  local summary rows total groups
  summary="$(python3 - "$ROOT/results/runs/E2E-11-window/output/result" <<'PY'
import glob
import json
import sys

rows = []
for path in glob.glob(sys.argv[1] + "/part-*.json"):
    with open(path, "r", encoding="utf-8") as handle:
        rows.extend(json.loads(line) for line in handle)
total = sum(int(row["order_count"]) for row in rows)
groups = {(row["window_start"], row["window_end"], row["city"]) for row in rows}
print(f"{len(rows)} {total} {len(groups)}")
PY
)"
  read -r rows total groups <<< "$summary"
  [[ "$rows" -eq 4 && "$total" -eq 600 && "$groups" -eq 4 ]] ||
    fail "window output rows=$rows total=$total groups=$groups (expected 4/600/4)"

  echo "== E2E-11-dedup: 1,000 unique keys + 100 duplicates =="
  run_case E2E-11-dedup --timeout 90 --allow-timeout
  summary="$(python3 - "$ROOT/results/runs/E2E-11-dedup/output/result" <<'PY'
import glob
import json
import sys

ids = []
for path in glob.glob(sys.argv[1] + "/part-*.json"):
    with open(path, "r", encoding="utf-8") as handle:
        ids.extend(int(json.loads(line)["order_id"]) for line in handle)
print(f"{len(ids)} {len(set(ids))}")
PY
)"
  read -r rows total <<< "$summary"
  [[ "$rows" -eq 1000 && "$total" -eq 1000 ]] ||
    fail "dedup output rows=$rows unique=$total (expected 1000/1000)"

  echo "E2E-11 PASS (window aggregate closed 4 windows; dedup emitted 1,000 unique keys)"
}

e2e_10_variant() {
  local variant="$1"
  local case_dir="$ROOT/results/runs/E2E-10-$variant"
  local table_path="$ROOT/results/state/e2e-10/$variant/pudding_test.db/e2e10_${variant}_orders"

  rm -rf "$ROOT/results/state/e2e-10/$variant" \
    "$ROOT/results/state/e2e-10/${variant}-checkpoint"
  "$ROOT/scripts/run/create-paimon-table.sh" \
    "$ROOT/results/state/e2e-10/$variant" pudding_test \
    "e2e10_${variant}_orders" avro

  echo "== E2E-10-$variant: decode 10,000 Confluent wire-format messages =="
  run_case "E2E-10-$variant" --timeout 150 --allow-timeout
  cp "$case_dir/submit.log" "$case_dir/run-1-decode.log"
  "$ROOT/scripts/verify/table-check.sh" paimon "$table_path" \
    --count 10000 \
    --exists "order_id = 1 AND amount = 0.37 AND city = 'xian'"
}

e2e_10() {
  "$ROOT/scripts/run/prepare-kafka-codec-smoke.sh"
  e2e_10_variant avro
  e2e_10_variant protobuf
  echo "E2E-10 PASS (Avro and Protobuf decoding through Schema Registry)"
}

e2e_09() {
  local state_dir="$ROOT/results/state/e2e-09"
  local hudi_path="$state_dir/hudi/orders"
  local mutation="$state_dir/mutation.jsonl"

  "$ROOT/scripts/run/prepare-kafka-smoke.sh"
  rm -rf "$state_dir"
  mkdir -p "$state_dir"
  "$ROOT/scripts/run/create-hudi-table.sh" "$hudi_path" e2e09_orders

  echo "== run 1: consume the 10,000-message topic =="
  run_case E2E-09 --timeout 180 --allow-timeout
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-1-initial.log"
  # 9,730 of the first 10,000 seed orders have amount >= 100.
  "$ROOT/scripts/verify/table-check.sh" hudi "$hudi_path" --count 9730

  echo "== produce 2 inserts and 1 update =="
  cat > "$mutation" <<'JSONL'
{"order_id":1000901,"customer_id":1,"amount":150.00,"status":"NEW","city":"hangzhou","order_time":"2026-11-06T12:00:00Z","updated_at":"2026-11-06T12:00:00Z","payload":"e2e09-insert-1"}
{"order_id":1000902,"customer_id":2,"amount":250.00,"status":"NEW","city":"chengdu","order_time":"2026-11-06T12:00:01Z","updated_at":"2026-11-06T12:00:01Z","payload":"e2e09-insert-2"}
{"order_id":271,"customer_id":13,"amount":222.22,"status":"DONE","city":"xian","order_time":"2026-01-01T01:21:18Z","updated_at":"2026-11-06T12:00:02Z","payload":"e2e09-updated"}
JSONL
  "$ROOT/.venv/bin/python" "$ROOT/scripts/run/produce-kafka-json.py" \
    --bootstrap "$KAFKA_BOOTSTRAP" --topic orders-smoke --file "$mutation"

  echo "== run 2: restart from checkpoint and consume the 3 new messages =="
  run_case E2E-09 --timeout 150 --allow-timeout
  cp "$CASE_DIR/submit.log" "$CASE_DIR/run-2-resume.log"
  "$ROOT/scripts/verify/table-check.sh" hudi "$hudi_path" \
    --count 9732 \
    --exists "order_id = 271 AND amount = 222.22 AND payload = 'e2e09-updated'" \
    --exists "order_id = 1000901" \
    --exists "order_id = 1000902"

  echo "E2E-09 PASS (filter/projection, checkpoint restart, upsert, no duplicates)"
}

mysql_target() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T \
    -e MYSQL_PWD="${MYSQL_ROOT_PASSWORD:-pudding}" mysql \
    mysql -uroot -N -B target_db -e "$1"
}

hive_sql() {
  "$SPARK_HOME/bin/spark-sql" --master 'local[2]' \
    --conf spark.sql.catalogImplementation=hive \
    --conf "spark.sql.warehouse.dir=$ROOT/data/hive/warehouse" \
    --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$ROOT/data/hive/metastore_db;create=true" \
    -e "$1" 2>/dev/null
}

verify_e2e14_count() {
  local sink_name="$1"
  local expected="$2"
  local rows
  case "$sink_name" in
    file)
      rows="$(json_rows "$ROOT/results/state/e2e-14/file")"
      ;;
    hive)
      rows="$(hive_sql "select count(*) from default.pudding_e2e14_hive" \
        | awk '/^[0-9]+$/{value=$0} END{print value}')"
      ;;
    jdbc)
      rows="$(psql_target "SELECT count(*) FROM e2e14_orders;")"
      ;;
    mysql)
      rows="$(mysql_target "SELECT count(*) FROM e2e14_orders;")"
      ;;
    hudi)
      "$ROOT/scripts/verify/table-check.sh" hudi \
        "$ROOT/results/state/e2e-14/hudi/orders" --count "$expected"
      return
      ;;
    paimon)
      "$ROOT/scripts/verify/table-check.sh" paimon \
        "$ROOT/results/state/e2e-14/paimon/pudding_test.db/e2e14_orders" \
        --count "$expected"
      return
      ;;
    iceberg)
      "$ROOT/scripts/verify/table-check.sh" iceberg \
        "$ROOT/data/generated/lakehouse/iceberg/pudding_test/e2e14_orders" \
        --count "$expected"
      return
      ;;
    *)
      fail "unknown E2E-14 sink: $sink_name"
      ;;
  esac
  [[ "$rows" -eq "$expected" ]] || fail "$sink_name rows $rows != $expected"
}

e2e_14() {
  rm -rf "$ROOT/results/state/e2e-14"
  psql_target "DROP TABLE IF EXISTS e2e14_orders;"
  mysql_target "DROP TABLE IF EXISTS e2e14_orders;"
  hive_sql "DROP TABLE IF EXISTS default.pudding_e2e14_hive"
  rm -rf "$ROOT/data/generated/lakehouse/iceberg/pudding_test/e2e14_orders"
  # The Paimon catalog creates tables but not namespaces.
  "$ROOT/scripts/run/create-paimon-database.sh" \
    "$ROOT/results/state/e2e-14/paimon" pudding_test

  local sink_name
  for sink_name in file hive jdbc mysql hudi paimon iceberg; do
    echo "== E2E-14-$sink_name: full refresh with 100 rows =="
    export E2E14_LIMIT=100
    run_case "E2E-14-$sink_name" --timeout 900
    verify_e2e14_count "$sink_name" 100

    echo "== E2E-14-$sink_name: full refresh with 200 rows =="
    export E2E14_LIMIT=200
    run_case "E2E-14-$sink_name" --timeout 900
    verify_e2e14_count "$sink_name" 200
  done
  unset E2E14_LIMIT

  echo "E2E-14 PASS (all sinks replaced 100 rows with 200 rows)"
}

e2e_02() {
  run_case E2E-02 --timeout 900
  local out="$CASE_DIR/output/result"
  local summary
  summary="$(python3 - "$out" <<'PY'
import glob
import json
import sys

rows = 0
missing = 0
for path in glob.glob(sys.argv[1] + "/part-*.json"):
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            rows += 1
            if not row.get("name") or not row.get("segment"):
                missing += 1
print(f"{rows} {missing}")
PY
)"
  local rows="${summary%% *}"
  local missing="${summary##* }"
  [[ "$rows" -eq 15384 ]] || fail "E2E-02 rows $rows != 15384"
  [[ "$missing" -eq 0 ]] || fail "E2E-02 joined rows missing customer fields: $missing"
  echo "E2E-02 PASS (union, inner join, projection and dedup produced 15,384 rows)"
}

e2e_03() {
  run_case E2E-03 --timeout 900
  local valid invalid
  {
    read -r valid
    read -r invalid
  } < <(python3 - "$CASE_DIR/output/result" "$CASE_DIR/output/quarantine" <<'PY'
import glob
import json
import sys


def ids(root):
    values = set()
    for path in glob.glob(root + "/part-*.json"):
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                values.add(int(json.loads(line)["order_id"]))
    return " ".join(str(value) for value in sorted(values))


print(ids(sys.argv[1]))
print(ids(sys.argv[2]))
PY
)
  [[ "$valid" == "1 2 6 7 8 9 10" ]] || fail "E2E-03 valid order ids: '$valid'"
  [[ "$invalid" == "3 4 5" ]] || fail "E2E-03 quarantined order ids: '$invalid'"
  echo "E2E-03 PASS (7 valid rows, 3 quarantined rows, schema contract applied)"
}

e2e_06_scale() {
  local path="$ROOT/results/state/e2e-06-scale/hudi/orders"
  local expected
  expected="$(mysql_source "SELECT COUNT(*) FROM orders;")"
  rm -rf "$ROOT/results/state/e2e-06-scale"

  echo "== E2E-06-scale: $expected-row MySQL initial snapshot into Hudi =="
  # The snapshot is CPU/IO heavy on a laptop; the sink uses DIRECT markers so
  # the local Hudi marker HTTP server cannot invalidate a long run.
  run_case E2E-06-hudi-mysql-cdc-scale \
    --timeout "${E2E06_SCALE_TIMEOUT:-4800}" --allow-timeout
  local case_dir="$ROOT/results/runs/E2E-06-hudi-mysql-cdc-scale"
  cp "$case_dir/submit.log" "$case_dir/snapshot.log"
  env -u HADOOP_CONF_DIR "$ROOT/scripts/verify/table-check.sh" hudi "$path" \
    --count "$expected" ||
    fail "CDC snapshot row count did not match MySQL ($expected)"
  echo "E2E-06-scale PASS ($expected-row CDC snapshot into Hudi)"
}

case "$CASE_ID" in
  E2E-01) e2e_01 ;;
  E2E-02) e2e_02 ;;
  E2E-03) e2e_03 ;;
  E2E-04) e2e_04 ;;
  E2E-04-scale) e2e_04_scale ;;
  E2E-05) e2e_05 ;;
  E2E-06) e2e_06 ;;
  E2E-06-scale) e2e_06_scale ;;
  E2E-07) e2e_07 ;;
  E2E-08) e2e_08 ;;
  E2E-09) e2e_09 ;;
  E2E-10) e2e_10 ;;
  E2E-11) e2e_11 ;;
  E2E-12) e2e_12 ;;
  E2E-12b) e2e_12b ;;
  E2E-13) e2e_13 ;;
  E2E-14) e2e_14 ;;
  *)
    echo "no orchestration for $CASE_ID" >&2
    exit 2
    ;;
esac
