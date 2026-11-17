#!/usr/bin/env bash
#
# OS-03: HDFS checkpoints and incremental state, including a NameNode/DataNode
# restart between runs.
#
# Usage: scripts/run/run-hdfs.sh
#
# The hdfs profile must already be running. The runner creates its own HDFS
# directories, waits for safe mode to clear after a restart, and verifies that
# the checkpoint/state survive the restart.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
STATE_DIR="$ROOT/results/state/os-03"
CONF_DIR="$STATE_DIR/hadoop-conf"
SUMMARY="$ROOT/results/runs/hdfs-summary.md"
NAMENODE="pudding-test-namenode-1"
HDFS_PORT="${HDFS_NAMENODE_PORT:-18020}"

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
echo "# HDFS cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

hdfs() {
  docker compose -f "$COMPOSE_FILE" --profile hdfs exec -T namenode hdfs "$@" 2>/dev/null
}

rows() {
  python3 - "$1" <<'PY'
import glob
import sys
print(sum(sum(1 for _ in open(path, encoding="utf-8"))
          for path in glob.glob(sys.argv[1] + "/**/part-*.json", recursive=True)))
PY
}

wait_hdfs() {
  local status
  for _ in $(seq 1 180); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "$NAMENODE" 2>/dev/null || true)"
    [[ "$status" == "healthy" ]] && break
    sleep 2
  done
  [[ "$status" == "healthy" ]] || fail "NameNode did not become healthy"

  # A restarted NameNode stays in safe mode until the DataNode reports. Wait
  # for the actual mode instead of relying on the container health check.
  for _ in $(seq 1 180); do
    if hdfs dfsadmin -safemode get | grep -q "Safe mode is OFF"; then
      return 0
    fi
    sleep 2
  done
  fail "NameNode stayed in safe mode"
}

restart_hdfs() {
  docker compose -f "$COMPOSE_FILE" --profile hdfs restart namenode datanode \
    >/dev/null 2>&1
  wait_hdfs
}

if ! docker ps --format '{{.Names}}' | grep -qx "$NAMENODE"; then
  "$ROOT/scripts/services/up.sh" hdfs >/dev/null
fi
wait_hdfs

hdfs dfs -mkdir -p /pudding/checkpoints/os-03 /pudding/state/os-03 >/dev/null
hdfs dfs -chmod -R 777 /pudding >/dev/null
hdfs dfs -rm -r -f /pudding/checkpoints/os-03/streaming >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /pudding/state/os-03/incremental >/dev/null 2>&1 || true

umask 077
cat > "$CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:$HDFS_PORT</value></property>
</configuration>
EOF
cat > "$CONF_DIR/hdfs-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
</configuration>
EOF
chmod 600 "$CONF_DIR"/*.xml

rm -rf "$STATE_DIR/input" "$STATE_DIR/output"
mkdir -p "$STATE_DIR/input"
{
  head -1 "$ROOT/data/seed/e2e03_orders.csv"
  tail -n +2 "$ROOT/data/seed/e2e03_orders.csv" | head -10
} > "$STATE_DIR/input/batch1.csv"

# --------------------------------------------------------- checkpoint on HDFS
"$ROOT/scripts/run/run-case.sh" OS-03-hdfs-checkpoint --timeout 900 \
  >>"$ROOT/results/runs/hdfs-runner.log" 2>&1 || fail "HDFS checkpoint first run failed"
[[ "$(rows "$STATE_DIR/output")" -eq 10 ]] ||
  fail "HDFS checkpoint first run did not write 10 rows"
pass "OS-03 first streaming run wrote 10 rows with a checkpoint on HDFS"

"$ROOT/scripts/run/run-case.sh" OS-03-hdfs-checkpoint --timeout 900 \
  >>"$ROOT/results/runs/hdfs-runner.log" 2>&1 || fail "HDFS checkpoint restart failed"
[[ "$(rows "$STATE_DIR/output")" -eq 10 ]] ||
  fail "HDFS checkpoint restart reprocessed files"
pass "OS-03 restart resumed from the HDFS checkpoint without duplicates"

echo "== restarting NameNode and DataNode =="
restart_hdfs
cat > "$STATE_DIR/input/batch2.csv" <<'CSV'
order_id,customer_id,amount,status,city,order_time,updated_at,payload
6001,1,10.00,PAID,shanghai,2026-01-03T00:00:01Z,2026-01-03T00:00:01Z,os03-batch2-1
6002,2,20.00,PAID,beijing,2026-01-03T00:00:02Z,2026-01-03T00:00:02Z,os03-batch2-2
CSV
"$ROOT/scripts/run/run-case.sh" OS-03-hdfs-checkpoint --timeout 900 \
  >>"$ROOT/results/runs/hdfs-runner.log" 2>&1 || fail "HDFS checkpoint after restart failed"
[[ "$(rows "$STATE_DIR/output")" -eq 12 ]] ||
  fail "HDFS checkpoint after restart did not process the new file"
pass "OS-03 processed 2 new rows after an HDFS restart (12 total)"

checkpoint_json="$("$PUDDING_HOME/bin/pudding" checkpoint status \
  "hdfs://localhost:$HDFS_PORT/pudding/checkpoints/os-03/streaming" \
  --json --hadoop-conf "$CONF_DIR" 2>/dev/null)"
python3 - "$checkpoint_json" <<'PY' || fail "HDFS checkpoint status is incomplete"
import json
import sys
value = json.loads(sys.argv[1])
assert value["checkpoint"] is True, value
assert value["commits"] == 2 and value["offsets"] == 2, value
assert value["lastCommit"] == "1", value
assert value["ageSeconds"] >= 0, value
PY
"$PUDDING_HOME/bin/pudding" checkpoint cleanup \
  "hdfs://localhost:$HDFS_PORT/pudding/checkpoints/os-03" \
  --older-than 1d --dry-run --hadoop-conf "$CONF_DIR" 2>/dev/null |
  grep -q "no checkpoints selected" || fail "HDFS checkpoint cleanup selected a recent checkpoint"
pass "OS-03 checkpoint CLI reads HDFS metadata and computes age"

# ------------------------------------------------------------ state on HDFS
docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
  psql -U pudding -d target_db -v ON_ERROR_STOP=1 -c "TRUNCATE os03_orders;" >/dev/null
docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
  psql -U pudding -d source_db -v ON_ERROR_STOP=1 -c \
  "DELETE FROM os05_orders WHERE order_id > 50;" >/dev/null

"$ROOT/scripts/run/run-case.sh" OS-03-hdfs-state --timeout 900 \
  >>"$ROOT/results/runs/hdfs-runner.log" 2>&1 || fail "HDFS state first run failed"
count="$(docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
  psql -U pudding -d target_db -At -c "SELECT count(*) FROM os03_orders;")"
[[ "$count" -eq 50 ]] || fail "HDFS state first run wrote $count rows, expected 50"
pass "OS-03 incremental first run wrote 50 rows with state on HDFS"

docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
  psql -U pudding -d source_db -v ON_ERROR_STOP=1 -c \
  "INSERT INTO os05_orders VALUES (51,51,51.00,'PAID','shanghai',
     timestamp '2026-02-01 03:00:00', timestamp '2026-02-01 03:00:00', 'os05-51');" \
  >/dev/null
echo "== restarting HDFS before the incremental resume =="
restart_hdfs
"$ROOT/scripts/run/run-case.sh" OS-03-hdfs-state --timeout 900 \
  >>"$ROOT/results/runs/hdfs-runner.log" 2>&1 || fail "HDFS state resume failed"
count="$(docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
  psql -U pudding -d target_db -At -c \
  "SELECT count(*) || '/' || count(DISTINCT order_id) FROM os03_orders;")"
[[ "$count" == "51/51" ]] ||
  fail "HDFS state resume produced rows/distinct=$count, expected 51/51"
pass "OS-03 incremental state survived the HDFS restart (51 rows, no duplicates)"

state_json="$(HADOOP_CONF_DIR="$CONF_DIR" "$PUDDING_HOME/bin/pudding" state status \
  "hdfs://localhost:$HDFS_PORT/pudding/state/os-03/incremental" --json 2>/dev/null)"
python3 - "$state_json" <<'PY' || fail "HDFS state status is incomplete"
import json
import sys
value = json.loads(sys.argv[1])
assert len(value) == 1, value
source = value[0]
assert source["sourceId"] == "os03_source", source
assert source["pending"] is None, source
assert source["lock"] is None, source
assert source["fencingToken"] >= 2, source
assert source["committedCursor"], source
PY
pass "OS-03 state CLI reads a committed cursor and advanced fencing token from HDFS"

echo >> "$SUMMARY"
echo "ALL PASS: OS-03 (HDFS checkpoint/state and restart recovery)" >> "$SUMMARY"
echo
echo "ALL PASS: OS-03 (HDFS checkpoint/state and restart recovery)"
echo "summary: ${SUMMARY#$ROOT/}"
