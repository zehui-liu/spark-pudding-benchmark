#!/usr/bin/env bash
#
# OS-05: JDBC, Redis and etcd incremental state backends.
#
# For each backend the runner:
#   1. clears the backend and the target table;
#   2. runs an initial incremental sync (50 rows);
#   3. inserts one source row and reruns (51 rows, no duplicates);
#   4. asserts state status JSON, lock release and the backend's stored key;
# and additionally exercises `state migrate` (JDBC), reset and restore.
#
# Usage: scripts/run/run-state-backends.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
STATE_DIR="$ROOT/results/state/os-05"
SUMMARY="$ROOT/results/runs/state-backends-summary.md"

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

mkdir -p "$STATE_DIR" "$ROOT/results/runs"
: > "$SUMMARY"
echo "# State backend cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

psql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d source_db -v ON_ERROR_STOP=1 -At -c "$1"
}

psql_target() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d target_db -v ON_ERROR_STOP=1 -At -c "$1"
}

state_cli() {
  "$PUDDING_HOME/bin/pudding" state "$@" 2>/dev/null
}

run_backend() {
  local backend="$1" template="$2" properties="$3"
  local target_rows distinct_rows status_json cursor
  echo
  echo "== backend: $backend =="

  psql_source "DELETE FROM os05_orders WHERE order_id > 50;" >/dev/null
  psql_target "TRUNCATE os05_orders;" >/dev/null

  if [[ "$backend" == "jdbc" ]]; then
    psql_source "DROP TABLE IF EXISTS pudding_os05_state;" >/dev/null
    state_cli migrate "@$properties" | grep -q "migrated incremental state table" ||
      fail "state migrate did not create the JDBC table"
    state_cli migrate "@$properties" | grep -q "migrated incremental state table" ||
      fail "state migrate is not idempotent"
    pass "OS-05 JDBC state migrate creates and upgrades the table"
  elif [[ "$backend" == "redis" ]]; then
    docker compose -f "$COMPOSE_FILE" --profile infra exec -T redis \
      redis-cli --scan --pattern 'pudding:os05:*' |
      while read -r key; do
        if [[ -n "$key" ]]; then
          docker compose -f "$COMPOSE_FILE" --profile infra exec -T redis \
            redis-cli del "$key" >/dev/null
        fi
      done
  elif [[ "$backend" == "etcd" ]]; then
    docker compose -f "$COMPOSE_FILE" --profile infra exec -T etcd \
      etcdctl --endpoints=http://127.0.0.1:2379 del --prefix /pudding/os05/ >/dev/null
  fi

  "$ROOT/scripts/run/run-case.sh" "$template" --timeout 900 >>"$ROOT/results/runs/state-backends-runner.log" 2>&1 ||
    fail "$backend first run failed"
  target_rows="$(psql_target "SELECT count(*) FROM os05_orders;")"
  [[ "$target_rows" -eq 50 ]] || fail "$backend first run wrote $target_rows rows, expected 50"
  pass "OS-05 $backend initial sync wrote 50 rows"

  psql_source "
    INSERT INTO os05_orders
    VALUES (51, 51, 51.00, 'PAID', 'shanghai',
            timestamp '2026-02-01 03:00:00', timestamp '2026-02-01 03:00:00', 'os05-51');
  " >/dev/null
  "$ROOT/scripts/run/run-case.sh" "$template" --timeout 900 >>"$ROOT/results/runs/state-backends-runner.log" 2>&1 ||
    fail "$backend resume run failed"
  target_rows="$(psql_target "SELECT count(*) FROM os05_orders;")"
  distinct_rows="$(psql_target "SELECT count(DISTINCT order_id) FROM os05_orders;")"
  [[ "$target_rows" -eq 51 && "$distinct_rows" -eq 51 ]] ||
    fail "$backend resume produced rows=$target_rows distinct=$distinct_rows, expected 51/51"
  pass "OS-05 $backend resumed from committed state and applied 1 new row (51 total)"

  status_json="$(state_cli status "@$properties" --json)"
  python3 - "$backend" "$status_json" <<'PY' || fail "$backend state status is inconsistent"
import json
import sys

backend, raw = sys.argv[1], sys.argv[2]
value = json.loads(raw)
if len(value) != 1:
    raise SystemExit(f"{backend}: expected one source, got {len(value)}")
source = value[0]
if source["sourceId"] != "os05_source":
    raise SystemExit(f"{backend}: unexpected source {source['sourceId']}")
if not source["committedCursor"]:
    raise SystemExit(f"{backend}: missing committed cursor")
if source["pending"] is not None:
    raise SystemExit(f"{backend}: pending transaction remains")
if source["lock"] is not None:
    raise SystemExit(f"{backend}: lock was not released")
if source.get("fencingToken", 0) < 2:
    raise SystemExit(f"{backend}: fencing token did not advance: {source}")
PY
  pass "OS-05 $backend state status has a committed cursor, fencing token and no lock"

  state_cli unlock "@$properties" os05_source |
    grep -q "no incremental state lock found" ||
    fail "$backend unlock did not report a released lock"

  if [[ "$backend" == "jdbc" ]]; then
    cursor="$(python3 - "$status_json" <<'PY'
import json
import sys
print(json.loads(sys.argv[1])[0]["committedCursor"])
PY
)"
    state_cli reset "@$properties" os05_source --cursor "2000-01-01 00:00:00" --force >/dev/null
    state_cli show "@$properties" os05_source |
      grep -q "2000-01-01 00:00:00" || fail "JDBC state reset did not change the cursor"
    state_cli reset "@$properties" os05_source --cursor "$cursor" --force >/dev/null
    state_cli show "@$properties" os05_source |
      grep -qF "$cursor" || fail "JDBC state reset did not restore the cursor"
    pass "OS-05 JDBC state reset changes and restores the cursor"
  fi
}

if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-postgres-1'; then
  "$ROOT/scripts/services/up.sh" relational >/dev/null
fi
if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-redis-1' ||
   ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-etcd-1'; then
  "$ROOT/scripts/services/up.sh" infra >/dev/null
fi

umask 077
cat > "$STATE_DIR/jdbc.properties" <<EOF
backend=jdbc
url=jdbc:postgresql://localhost:${PG_PORT:-15432}/source_db
user=${PG_USER:-pudding}
password=${PG_PASSWORD:-pudding}
table=pudding_os05_state
lockTtlMs=600000
EOF
cat > "$STATE_DIR/redis.properties" <<EOF
backend=redis
mode=single
host=localhost
port=${REDIS_PORT:-16379}
prefix=pudding:os05:
lockTtlMs=600000
EOF
cat > "$STATE_DIR/etcd.properties" <<EOF
backend=etcd
endpoint=http://localhost:${ETCD_PORT:-12379}
prefix=/pudding/os05/
lockTtlMs=600000
EOF
chmod 600 "$STATE_DIR"/*.properties

run_backend jdbc OS-05-state-jdbc "$STATE_DIR/jdbc.properties"
psql_source "SELECT count(*) FROM pudding_os05_state;" | grep -qx '1' ||
  fail "JDBC state table does not contain the source row"

run_backend redis OS-05-state-redis "$STATE_DIR/redis.properties"
docker compose -f "$COMPOSE_FILE" --profile infra exec -T redis \
  redis-cli --scan --pattern 'pudding:os05:*' | grep -q ':os05_source:state' ||
  fail "Redis state key is missing"

run_backend etcd OS-05-state-etcd "$STATE_DIR/etcd.properties"
docker compose -f "$COMPOSE_FILE" --profile infra exec -T etcd \
  etcdctl --endpoints=http://127.0.0.1:2379 get --prefix /pudding/os05/ --keys-only |
  grep -q '/pudding/os05/os05_source/state' || fail "etcd state key is missing"

echo >> "$SUMMARY"
echo "ALL PASS: OS-05 (JDBC, Redis and etcd state backends)" >> "$SUMMARY"
echo
echo "ALL PASS: OS-05 (JDBC, Redis and etcd state backends)"
echo "summary: ${SUMMARY#$ROOT/}"
