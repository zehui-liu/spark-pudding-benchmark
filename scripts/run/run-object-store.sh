#!/usr/bin/env bash
#
# E2E-15 / OS-01 / OS-02 / OS-04: object-storage checkpoints, incremental state,
# Iceberg S3FileIO and the checkpoint maintenance CLI.
#
# Usage:
#   scripts/run/run-object-store.sh
#
# The script resets the object-store prefixes and PostgreSQL fixtures, then:
#   1. streams a file into a local sink with the checkpoint on S3A and restarts it;
#   2. runs an incremental JDBC sync with its state backend on S3A;
#   3. writes and reads an Iceberg table through S3FileIO and verifies that the
#      S3A (checkpoint/state) and S3FileIO (warehouse) credentials are separate;
#   4. exercises checkpoint show/status/delete/cleanup, including dry runs and
#      age-based selection.
#
# Requires the `infra` profile (MinIO) and PostgreSQL. The runner starts them if
# they are not running.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
STATE_DIR="$ROOT/results/state/object-store"
SECRET_FILE="$STATE_DIR/secrets.properties"
CONF_STATE="$STATE_DIR/checkpoint-state.properties"
CONF_LAKE="$STATE_DIR/checkpoint-lake.properties"
RUN_DIR="$ROOT/results/runs/object-store"
SUMMARY="$ROOT/results/runs/object-store-summary.md"

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"
SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi
export SPARK_HOME PUDDING_HOME

mkdir -p "$RUN_DIR"
: > "$SUMMARY"
echo "# Object-storage cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

run_case_ok() {
  local id="$1"
  shift
  if ! "$ROOT/scripts/run/run-case.sh" "$id" "$@" >>"$RUN_DIR/runner.log" 2>&1; then
    tail -30 "$ROOT/results/runs/$id/submit.log" >&2 || true
    fail "$id failed"
  fi
}

run_case_expect_failure() {
  local id="$1" destination="$2"
  shift 2
  set +e
  "$ROOT/scripts/run/run-case.sh" "$id" "$@" >>"$RUN_DIR/runner.log" 2>&1
  local code=$?
  set -e
  [[ "$code" -ne 0 ]] || fail "$id unexpectedly succeeded"
  cp "$ROOT/results/runs/$id/submit.log" "$destination"
}

rows() {
  python3 - "$1" <<'PY'
import glob
import os
import sys
files = glob.glob(os.path.join(sys.argv[1], "**", "part-*"), recursive=True)
print(sum(sum(1 for _ in open(path, encoding="utf-8")) for path in files))
PY
}

pg_count() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d target_db -At -c "SELECT count(*) FROM $1;"
}

pg_distinct() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d target_db -At -c "SELECT count(DISTINCT $2) FROM $1;"
}

psql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d source_db -v ON_ERROR_STOP=1 -At -c "$1"
}

secret_value() {
  awk -F= -v key="$1" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$SECRET_FILE"
}

secret_ref() {
  printf '%s' '${secret:file:'"$SECRET_FILE"'#'"$1"'}'
}

checkpoint_cli() {
  "$PUDDING_HOME/bin/pudding" checkpoint "$@" --conf-file "$CONF_STATE" 2>/dev/null
}

checkpoint_json() {
  checkpoint_cli "$@" --json
}

state_cli() {
  HADOOP_CONF_DIR="$STATE_DIR/hadoop-conf" \
    AWS_ACCESS_KEY_ID="$(secret_value state-access)" \
    AWS_SECRET_ACCESS_KEY="$(secret_value state-secret)" \
    "$PUDDING_HOME/bin/pudding" state "$@" 2>/dev/null
}

mc_root() {
  docker compose -f "$COMPOSE_FILE" --profile infra run --rm -T --no-deps \
    --entrypoint /bin/sh minio-init -c \
    "mc alias set local http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null
     $1" 2>/dev/null
}

expect_failure() {
  local name="$1"
  shift
  local output
  set +e
  output="$("$@" 2>&1)"
  local code=$?
  set -e
  [[ "$code" -ne 0 ]] || fail "$name unexpectedly succeeded"
  printf '%s' "$output"
}

expect_message() {
  local name="$1" expected="$2" output="$3"
  grep -qF "$expected" <<<"$output" || fail "$name did not report: $expected"
}

# ---------------------------------------------------------------- environment
if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-minio-1'; then
  "$ROOT/scripts/services/up.sh" infra >/dev/null
fi
if ! docker ps --format '{{.Names}}' | grep -qx 'pudding-test-postgres-1'; then
  "$ROOT/scripts/services/up.sh" relational >/dev/null
fi

"$ROOT/scripts/run/prepare-object-store.sh" --reset >>"$RUN_DIR/runner.log" 2>&1 \
  || fail "object-store preparation failed"
pass "object-store fixtures prepared (scoped MinIO users, 0600 secret files)"

state_secret="$(secret_value state-secret)"
lake_secret="$(secret_value lake-secret)"

# ------------------------------------------------------- OS-01 streaming S3A
run_case_ok OS-01-s3a-checkpoint --timeout 600
[[ "$(rows "$ROOT/results/state/os-01/output")" -eq 10 ]] \
  || fail "OS-01 first streaming run did not write 10 rows"
pass "OS-01 streaming wrote 10 rows with a checkpoint on S3A"

run_case_ok OS-01-s3a-checkpoint --timeout 600
[[ "$(rows "$ROOT/results/state/os-01/output")" -eq 10 ]] \
  || fail "OS-01 restart reprocessed files instead of resuming from the S3A checkpoint"
pass "OS-01 restart resumed from the S3A checkpoint without duplicates"

cat > "$ROOT/data/generated/os01/batch2.csv" <<'CSV'
order_id,customer_id,amount,status,city,order_time,updated_at,payload
5001,1,10.00,PAID,shanghai,2026-01-02T00:00:01Z,2026-01-02T00:00:01Z,os01-batch2-1
5002,2,20.00,PAID,beijing,2026-01-02T00:00:02Z,2026-01-02T00:00:02Z,os01-batch2-2
CSV
run_case_ok OS-01-s3a-checkpoint --timeout 600
[[ "$(rows "$ROOT/results/state/os-01/output")" -eq 12 ]] \
  || fail "OS-01 did not process the new file added after the checkpoint"
pass "OS-01 processed 2 new rows after restart (12 total, no duplicates)"

status_json="$(checkpoint_json status s3a://pudding/checkpoints/os-01/streaming)"
python3 - "$status_json" <<'PY' || fail "OS-01 checkpoint status fields are incomplete"
import json
import sys
value = json.loads(sys.argv[1])
assert value["checkpoint"] is True, value
assert value["exists"] is True, value
assert value["commits"] == 2, value
assert value["offsets"] == 2, value
assert value["lastCommit"] == "1", value
assert value["sizeBytes"] > 0, value
PY
pass "OS-01 S3A checkpoint status reports commits, offsets and size"

# ------------------------------------------------------ OS-01 incremental state
export OS01_ALLOW_BEST_EFFORT=false
run_case_expect_failure OS-01-s3a-state "$RUN_DIR/os-01-state-gate.log" --timeout 600
expect_message "object-store state safety gate" \
  "filesystem incremental state on object store scheme 's3a' has only a best-effort lock" \
  "$(cat "$RUN_DIR/os-01-state-gate.log")"
pass "OS-01 rejects S3A incremental state without allowBestEffortLock"

export OS01_ALLOW_BEST_EFFORT=true
run_case_ok OS-01-s3a-state --timeout 600
[[ "$(pg_count os01_orders)" -eq 100 ]] \
  || fail "OS-01 incremental first run did not write 100 rows"
pass "OS-01 incremental first run wrote 100 rows with state on S3A"

psql_source "
  INSERT INTO os01_orders
  SELECT g, g % 1000, (g % 500)::numeric(12, 2), 'PAID', 'shanghai',
         timestamp '2026-01-01 00:00:00' + (g || ' seconds')::interval,
         timestamp '2026-01-01 00:00:00' + (g || ' seconds')::interval,
         'os01-' || g
  FROM generate_series(101, 102) AS g;
" >/dev/null
run_case_ok OS-01-s3a-state --timeout 600
[[ "$(pg_count os01_orders)" -eq 102 ]] \
  || fail "OS-01 incremental resume did not apply 2 new rows"
[[ "$(pg_distinct os01_orders order_id)" -eq 102 ]] \
  || fail "OS-01 incremental resume produced duplicate keys"
pass "OS-01 incremental resume from S3A state applied 2 new rows (102 total)"

state_json="$(state_cli status "@$STATE_DIR/state-s3a.properties" --json)"
python3 - "$state_json" <<'PY' || fail "OS-01 S3A state status is not committed"
import json
import sys
value = json.loads(sys.argv[1])
assert len(value) == 1, value
source = value[0]
assert source["sourceId"] == "os01_source", source
assert source["pending"] is None, source
assert source["lock"] is None, source
assert source["committedCursor"], source
PY
pass "OS-01 state status on S3A shows a committed cursor with no pending write or lock"

# ------------------------------------------------------------- OS-02 S3FileIO
iceberg_jar="$(ls "$PUDDING_HOME"/libs/iceberg-runtime-*.jar | head -1)"
"$SPARK_HOME/bin/spark-sql" --master 'local[2]' \
  --conf spark.sql.catalogImplementation=in-memory \
  --jars "$iceberg_jar" \
  --properties-file "$STATE_DIR/spark-catalog.properties" \
  -e "CREATE NAMESPACE IF NOT EXISTS iceberg_s3.os02" \
  >>"$RUN_DIR/runner.log" 2>&1 || fail "could not create the Iceberg namespace"

# The warehouse policy does not allow the checkpoint/state user. A write with
# those credentials must fail in the S3FileIO data layer, not silently borrow
# the S3A configuration.
export OS_LAKE_ACCESS_KEY_REF="$(secret_ref state-access)"
export OS_LAKE_SECRET_KEY_REF="$(secret_ref state-secret)"
export OS02_TABLE=os02.denied
export OS02_WAREHOUSE=s3://pudding/warehouse/os-02-denied
run_case_expect_failure OS-02-iceberg-s3fileio-write "$RUN_DIR/os-02-denied.log" --timeout 900
grep -qE "Access Denied|403 Forbidden" "$RUN_DIR/os-02-denied.log" \
  || fail "S3FileIO did not surface an access denial for the wrong credentials"
pass "OS-02 S3FileIO uses its own credentials and denies the S3A state user"
unset OS_LAKE_ACCESS_KEY_REF OS_LAKE_SECRET_KEY_REF OS02_TABLE OS02_WAREHOUSE

run_case_ok OS-02-iceberg-s3fileio-write --timeout 900
run_case_ok OS-02-iceberg-s3fileio-read --timeout 900
[[ "$(rows "$ROOT/results/runs/OS-02-iceberg-s3fileio-read/output/read")" -eq 1000 ]] \
  || fail "OS-02 Iceberg read did not return 1000 rows"
pass "OS-02 wrote and read 1000 rows through an Iceberg JDBC catalog and S3FileIO"

warehouse_objects="$(mc_root 'mc ls -r local/pudding/warehouse/os-02 | wc -l')"
[[ "${warehouse_objects//[[:space:]]/}" -ge 3 ]] \
  || fail "OS-02 warehouse has too few objects: $warehouse_objects"
pass "OS-02 wrote Iceberg data and metadata files under s3://pudding/warehouse/os-02"

state_to_warehouse="$(expect_failure "state credential reading the warehouse" \
  "$PUDDING_HOME/bin/pudding" checkpoint show s3a://pudding/warehouse/os-02 \
  --json --conf-file "$CONF_STATE")"
expect_message "state credential isolation" "403" "$state_to_warehouse"
lake_to_checkpoint="$(expect_failure "lake credential reading a checkpoint" \
  "$PUDDING_HOME/bin/pudding" checkpoint show s3a://pudding/checkpoints/os-01/streaming \
  --json --conf-file "$CONF_LAKE")"
expect_message "lake credential isolation" "403" "$lake_to_checkpoint"
pass "OS-02 S3A and S3FileIO credentials are isolated in both directions"

for file in "$ROOT"/results/runs/OS-*/submit.log "$ROOT"/results/runs/OS-*/job.yaml; do
  [[ -f "$file" ]] || continue
  if grep -qF "$state_secret" "$file" || grep -qF "$lake_secret" "$file"; then
    fail "secret value leaked into ${file#$ROOT/}"
  fi
done
pass "OS-02 job configurations and logs contain secret references only"

# ---------------------------------------------------------- OS-04 checkpoint CLI
OS01_CHECKPOINT=s3a://pudding/checkpoints/os-01/extra \
  OS01_OUTPUT="$ROOT/results/state/os-04/output-extra" \
  run_case_ok OS-01-s3a-checkpoint --timeout 600

streaming_json="$(checkpoint_json status s3a://pudding/checkpoints/os-01/streaming)"
extra_json="$(checkpoint_json status s3a://pudding/checkpoints/os-01/extra)"
python3 - "$streaming_json" "$extra_json" <<'PY' || fail "OS-04 checkpoint JSON is incomplete"
import json
import sys
streaming = json.loads(sys.argv[1])
extra = json.loads(sys.argv[2])
assert streaming["checkpoint"] is True, streaming
assert extra["checkpoint"] is True, extra
assert streaming["ageSeconds"] >= 5, streaming
assert streaming["lastModified"] != extra["lastModified"], (streaming, extra)
PY
pass "OS-04 status reports real file age for S3A checkpoints"

mc_root 'mc cp /etc/hostname local/pudding/checkpoints/os-01/not-a-checkpoint/x.txt >/dev/null 2>&1 || true'
non_checkpoint="$(expect_failure "delete of a non-checkpoint" \
  "$PUDDING_HOME/bin/pudding" checkpoint delete \
  s3a://pudding/checkpoints/os-01/not-a-checkpoint --conf-file "$CONF_STATE")"
expect_message "delete non-checkpoint" "not a Spark checkpoint" "$non_checkpoint"
pass "OS-04 refuses to delete a directory that is not a Spark checkpoint"

root_checkpoint="$(expect_failure "cleanup root that is a checkpoint" \
  "$PUDDING_HOME/bin/pudding" checkpoint cleanup \
  s3a://pudding/checkpoints/os-01/streaming --older-than 1d --conf-file "$CONF_STATE")"
expect_message "cleanup root guard" "cleanup root is itself a checkpoint" "$root_checkpoint"
pass "OS-04 refuses to use a checkpoint as the cleanup root"

printf 'spark.not.fs=1\n' > "$STATE_DIR/bad-conf.properties"
chmod 600 "$STATE_DIR/bad-conf.properties"
bad_conf="$(expect_failure "checkpoint conf with a non-fs key" \
  "$PUDDING_HOME/bin/pudding" checkpoint show \
  s3a://pudding/checkpoints/os-01/streaming --conf-file "$STATE_DIR/bad-conf.properties")"
expect_message "checkpoint conf validation" "only accepts fs.* keys" "$bad_conf"
pass "OS-04 rejects non-fs.* keys in --conf-file"

invalid_duration="$(expect_failure "invalid cleanup duration" \
  "$PUDDING_HOME/bin/pudding" checkpoint cleanup \
  s3a://pudding/checkpoints/os-01 --older-than 0s --conf-file "$CONF_STATE")"
expect_message "cleanup duration validation" "duration must be greater than zero" "$invalid_duration"
pass "OS-04 validates --older-than durations"

checkpoint_cli delete s3a://pudding/checkpoints/os-01/extra --dry-run \
  | grep -q "would delete" || fail "OS-04 delete --dry-run did not report the action"
checkpoint_json show s3a://pudding/checkpoints/os-01/extra >/dev/null \
  || fail "OS-04 delete --dry-run removed the checkpoint"
checkpoint_cli delete s3a://pudding/checkpoints/os-01/extra \
  | grep -q "deleted" || fail "OS-04 checkpoint delete failed"
missing="$(expect_failure "show of a deleted checkpoint" \
  "$PUDDING_HOME/bin/pudding" checkpoint show \
  s3a://pudding/checkpoints/os-01/extra --json --conf-file "$CONF_STATE")"
expect_message "deleted checkpoint" "does not exist" "$missing"
pass "OS-04 delete honors --dry-run and removes only the selected checkpoint"

OS01_CHECKPOINT=s3a://pudding/checkpoints/os-01/extra \
  OS01_OUTPUT="$ROOT/results/state/os-04/output-extra" \
  run_case_ok OS-01-s3a-checkpoint --timeout 600

keep_last_dry="$(checkpoint_cli cleanup s3a://pudding/checkpoints/os-01 \
  --keep-last 1 --dry-run)"
expect_message "cleanup keep-last" "would delete: s3a://pudding/checkpoints/os-01/streaming" \
  "$keep_last_dry"
checkpoint_json show s3a://pudding/checkpoints/os-01/streaming >/dev/null \
  || fail "OS-04 cleanup --dry-run removed a checkpoint"
checkpoint_json show s3a://pudding/checkpoints/os-01/extra >/dev/null \
  || fail "OS-04 cleanup --dry-run removed a checkpoint"
pass "OS-04 cleanup --keep-last selects the older checkpoint and honors --dry-run"

checkpoint_cli cleanup s3a://pudding/checkpoints/os-01 --keep-last 1 \
  | grep -q "deleted: s3a://pudding/checkpoints/os-01/streaming" \
  || fail "OS-04 cleanup kept the wrong checkpoint"
if checkpoint_json show s3a://pudding/checkpoints/os-01/streaming >/dev/null 2>&1; then
  fail "OS-04 cleanup did not delete the older checkpoint"
fi
checkpoint_json show s3a://pudding/checkpoints/os-01/extra >/dev/null \
  || fail "OS-04 cleanup deleted the newest checkpoint"
pass "OS-04 cleanup --keep-last retained the newest checkpoint"

older="$(checkpoint_cli cleanup s3a://pudding/checkpoints/os-01 --older-than 1d)"
expect_message "cleanup age threshold" "no checkpoints selected" "$older"
checkpoint_json show s3a://pudding/checkpoints/os-01/extra >/dev/null \
  || fail "OS-04 age cleanup deleted a recent checkpoint"
pass "OS-04 cleanup --older-than keeps checkpoints inside the age threshold"

checkpoint_cli cleanup s3a://pudding/checkpoints/os-01 --older-than 1ms \
  | grep -q "kept: 0" || fail "OS-04 age cleanup did not delete the expired checkpoint"
if checkpoint_json show s3a://pudding/checkpoints/os-01/extra >/dev/null 2>&1; then
  fail "OS-04 age cleanup did not delete the expired checkpoint"
fi
pass "OS-04 cleanup --older-than removed the expired checkpoint"

echo >> "$SUMMARY"
echo "ALL PASS: E2E-15 / OS-01 / OS-02 / OS-04" >> "$SUMMARY"
echo
echo "ALL PASS: E2E-15 / OS-01 / OS-02 / OS-04"
echo "summary: ${SUMMARY#$ROOT/}"
