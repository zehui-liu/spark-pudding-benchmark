#!/usr/bin/env bash
#
# Render one config/jobs/<case>.yaml and submit it through the bundled client.
#
# Usage:
#   scripts/run/run-case.sh SR-01
#   scripts/run/run-case.sh SK-03 --dataset smoke --timeout 600
#   scripts/run/run-case.sh TR-01 --dry-run
#
# The caller is responsible for starting the Docker profile a case needs:
#   scripts/services/up.sh relational   # JDBC/MySQL/MongoDB cases
#   scripts/services/up.sh streaming    # Kafka cases
#   scripts/services/up.sh search       # Elasticsearch cases
# File, Hive and lakehouse cases use no Docker profile.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JOBS="$ROOT/config/jobs"
COMPOSE_ENV="$ROOT/config/docker/.env"

case_id=""
dataset="smoke"
timeout_seconds=1800
dry_run=false
allow_timeout=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset) dataset="$2"; shift 2 ;;
    --timeout) timeout_seconds="$2"; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    --allow-timeout) allow_timeout=true; shift ;;
    -h|--help)
      sed -n '1,20p' "$0"
      exit 0
      ;;
    *) case_id="$1"; shift ;;
  esac
done

if [[ -z "$case_id" ]]; then
  echo "usage: $0 <case-id> [--dataset smoke|scale] [--timeout seconds] [--dry-run]" >&2
  exit 2
fi

# An exact template name wins, so several variants of one plan case can be
# addressed explicitly (for example OS-01-s3a-checkpoint and OS-01-s3a-state).
job_template="$(find "$JOBS" -maxdepth 1 -name "${case_id}.yaml" -print -quit)"
if [[ -z "$job_template" ]]; then
  job_template="$(find "$JOBS" -maxdepth 1 -name "${case_id}-*.yaml" -print -quit)"
fi
if [[ -z "$job_template" ]]; then
  echo "job template not found for case: $case_id" >&2
  exit 1
fi

if [[ "$dataset" == "smoke" ]]; then
  data_dir="$ROOT/data/seed"
elif [[ "$dataset" == "scale" ]]; then
  data_dir="$ROOT/data/generated"
else
  echo "unknown dataset: $dataset (expected smoke or scale)" >&2
  exit 2
fi

if [[ -f "$COMPOSE_ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$COMPOSE_ENV"
  set +a
fi
if [[ "${PUDDING_USE_HADOOP_CONF:-0}" != "1" ]]; then
  unset HADOOP_CONF_DIR
fi

SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"
SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi

case_dir="$ROOT/results/runs/$case_id"
case_out="$case_dir/output"
rm -rf "$case_dir"
mkdir -p "$case_out"

export SPARK_HOME PUDDING_HOME
export PUDDING_TEST_ROOT="$ROOT"
export PUDDING_TEST_DATA="$data_dir"
export CASE_OUT="$case_out"
export PG_URL="jdbc:postgresql://localhost:${PG_PORT:-15432}/source_db"
export PG_USER="${PG_USER:-pudding}"
export PG_PASSWORD="${PG_PASSWORD:-pudding}"
export PG_TARGET_URL="jdbc:postgresql://localhost:${PG_PORT:-15432}/target_db"
export MYSQL_URL="jdbc:mysql://localhost:${MYSQL_PORT:-13306}/source_db?useSSL=false&allowPublicKeyRetrieval=true"
export MYSQL_TARGET_URL="jdbc:mysql://localhost:${MYSQL_PORT:-13306}/target_db?useSSL=false&allowPublicKeyRetrieval=true"
export MYSQL_USER="${MYSQL_USER:-pudding}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-pudding}"
export MONGO_URI="mongodb://localhost:${MONGO_PORT:-27018}/?replicaSet=rs0"
export KAFKA_BOOTSTRAP="localhost:${KAFKA_PORT:-19092}"
export SCHEMA_REGISTRY_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-18081}"
export ES_PORT="${ES_PORT:-19200}"

# Append-mode lakehouse sinks accumulate rows across runs. Clean the stable
# target so the target assertion can expect an exact row count.
case "$case_id" in
  SK-11)
    rm -rf "$ROOT/data/generated/lakehouse/iceberg/pudding_test/sk11_orders"
    ;;
esac

rendered="$case_dir/job.yaml"
export E2E14_LIMIT="${E2E14_LIMIT:-100}"
export RG03_STATE_PATH="${RG03_STATE_PATH:-$ROOT/results/state/rg-03}"
export RG03_TARGET_TABLE="${RG03_TARGET_TABLE:-public.rg03_orders}"

# Object-storage cases keep credentials in a 0600 file and put only
# ${secret:file:...#key} references into the rendered job configuration.
object_store_dir="$ROOT/results/state/object-store"
export OS_SECRET_FILE="${OS_SECRET_FILE:-$object_store_dir/secrets.properties}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:${MINIO_PORT:-19000}}"
export OS01_INPUT="${OS01_INPUT:-$ROOT/data/generated/os01}"
export OS01_CHECKPOINT="${OS01_CHECKPOINT:-s3a://pudding/checkpoints/os-01/streaming}"
export OS01_OUTPUT="${OS01_OUTPUT:-$ROOT/results/state/os-01/output}"
export OS01_STATE_PATH="${OS01_STATE_PATH:-s3a://pudding/state/os-01/incremental}"
export OS01_ALLOW_BEST_EFFORT="${OS01_ALLOW_BEST_EFFORT:-true}"
export OS02_WAREHOUSE="${OS02_WAREHOUSE:-s3://pudding/warehouse/os-02}"
export OS02_TABLE="${OS02_TABLE:-os02.orders}"
export RG06_CHECKPOINT="${RG06_CHECKPOINT:-$ROOT/results/state/rg-06/checkpoint}"
export RG06_INPUT="${RG06_INPUT:-$ROOT/data/generated/os01}"
export RG06_TIMEOUT_MS="${RG06_TIMEOUT_MS:-3000}"
export OS03_INPUT="${OS03_INPUT:-$ROOT/results/state/os-03/input}"
export OS03_OUTPUT="${OS03_OUTPUT:-$ROOT/results/state/os-03/output}"
export OS03_CHECKPOINT="${OS03_CHECKPOINT:-hdfs://localhost:${HDFS_NAMENODE_PORT:-18020}/pudding/checkpoints/os-03/streaming}"
export OS03_STATE_PATH="${OS03_STATE_PATH:-hdfs://localhost:${HDFS_NAMENODE_PORT:-18020}/pudding/state/os-03/incremental}"
export AVRO_TOPIC="${AVRO_TOPIC:-orders-avro-custom}"
export AVRO_SUBJECT="${AVRO_SUBJECT:-orders-avro-custom-subject}"
export AVRO_CHECKPOINT="${AVRO_CHECKPOINT:-$ROOT/results/state/e2e-10-custom/checkpoint}"

secret_ref() {
  printf '%s' '${secret:file:'"$1"'#'"$2"'}'
}
export OS_STATE_ACCESS_KEY_REF="${OS_STATE_ACCESS_KEY_REF:-$(secret_ref "$OS_SECRET_FILE" state-access)}"
export OS_STATE_SECRET_KEY_REF="${OS_STATE_SECRET_KEY_REF:-$(secret_ref "$OS_SECRET_FILE" state-secret)}"
export OS_LAKE_ACCESS_KEY_REF="${OS_LAKE_ACCESS_KEY_REF:-$(secret_ref "$OS_SECRET_FILE" lake-access)}"
export OS_LAKE_SECRET_KEY_REF="${OS_LAKE_SECRET_KEY_REF:-$(secret_ref "$OS_SECRET_FILE" lake-secret)}"
export OS_CATALOG_USER_REF="${OS_CATALOG_USER_REF:-$(secret_ref "$OS_SECRET_FILE" catalog-user)}"
export OS_CATALOG_PASSWORD_REF="${OS_CATALOG_PASSWORD_REF:-$(secret_ref "$OS_SECRET_FILE" catalog-password)}"

envsubst '${SPARK_HOME} ${PUDDING_HOME} ${PUDDING_TEST_ROOT} ${PUDDING_TEST_DATA} ${CASE_OUT} ${PG_URL} ${PG_TARGET_URL} ${PG_USER} ${PG_PASSWORD} ${MYSQL_URL} ${MYSQL_TARGET_URL} ${MYSQL_USER} ${MYSQL_PASSWORD} ${MONGO_URI} ${KAFKA_BOOTSTRAP} ${SCHEMA_REGISTRY_URL} ${ES_PORT} ${E2E14_LIMIT} ${RG03_STATE_PATH} ${RG03_TARGET_TABLE} ${MINIO_ENDPOINT} ${OS01_INPUT} ${OS01_CHECKPOINT} ${OS01_OUTPUT} ${OS01_STATE_PATH} ${OS01_ALLOW_BEST_EFFORT} ${OS02_WAREHOUSE} ${OS02_TABLE} ${RG06_CHECKPOINT} ${RG06_INPUT} ${RG06_TIMEOUT_MS} ${OS03_INPUT} ${OS03_OUTPUT} ${OS03_CHECKPOINT} ${OS03_STATE_PATH} ${AVRO_TOPIC} ${AVRO_SUBJECT} ${AVRO_CHECKPOINT} ${PG_PORT} ${REDIS_PORT} ${ETCD_PORT} ${OS_STATE_ACCESS_KEY_REF} ${OS_STATE_SECRET_KEY_REF} ${OS_LAKE_ACCESS_KEY_REF} ${OS_LAKE_SECRET_KEY_REF} ${OS_CATALOG_USER_REF} ${OS_CATALOG_PASSWORD_REF}' \
  < "$job_template" > "$rendered"

env_file="$case_dir/env.properties"
{
  echo "JAVA_HOME=$JAVA_HOME"
  echo "SPARK_HOME=$SPARK_HOME"
  # Local cases must not inherit a cluster Hadoop config: fs.defaultFS may point at
  # HDFS (in this environment localhost:9000 is MinIO) and scheme-less state paths
  # would then use the default filesystem instead of the local one.
  if [[ "${PUDDING_USE_HADOOP_CONF:-0}" == "1" && -n "${HADOOP_CONF_DIR:-}" ]]; then
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
  fi
} > "$env_file"

# Benchmark mode: add Spark event logging to the rendered job without changing
# the reusable templates. Each invocation gets its own directory.
if [[ -n "${PUDDING_BENCH_EVENT_ROOT:-}" ]]; then
  export PUDDING_BENCH_EVENT_DIR="${PUDDING_BENCH_EVENT_ROOT}/${case_id}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p "$PUDDING_BENCH_EVENT_DIR"
  python3 - "$rendered" "$PUDDING_BENCH_EVENT_DIR" <<'PY'
import sys
import yaml

path, event_dir = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    job = yaml.safe_load(handle)
config = job.setdefault("job", {}).setdefault("config", {}) or {}
config["spark.eventLog.enabled"] = "true"
config["spark.eventLog.dir"] = "file://" + event_dir
job["job"]["config"] = config
with open(path, "w", encoding="utf-8") as handle:
    yaml.safe_dump(job, handle, sort_keys=False, allow_unicode=True)
PY
fi

echo "case:     $case_id"
echo "template: ${job_template#$ROOT/}"
echo "rendered: ${rendered#$ROOT/}"
echo "dataset:  $dataset"

if [[ "$dry_run" == "true" ]]; then
  echo "dry run; not submitting"
  exit 0
fi

log="$case_dir/submit.log"
started="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

# Run the submit in its own process group so a timeout kills the whole tree.
# The local client launches spark-submit as a child; killing only the client
# leaves the driver JVM running, which for CDC keeps a Debezium replica alive
# and makes the next run fail with MySQL error 1236 (duplicate server_id).
run_with_timeout() {
  local seconds="$1"
  shift
  set -m
  "$@" > "$log" 2>&1 &
  local pid=$!
  set +m
  local waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if [[ "$waited" -ge "$seconds" ]]; then
      kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
      local grace=0
      while kill -0 "$pid" 2>/dev/null && [[ "$grace" -lt 10 ]]; do
        sleep 1
        grace=$((grace + 1))
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
      fi
      wait "$pid" 2>/dev/null
      return 142
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "$pid"
}

set +e
if [[ "$timeout_seconds" -gt 0 ]]; then
  run_with_timeout "$timeout_seconds" \
    "$PUDDING_HOME/bin/pudding" submit "$env_file" "$rendered"
  code=$?
else
  "$PUDDING_HOME/bin/pudding" submit "$env_file" "$rendered" > "$log" 2>&1
  code=$?
fi
# A process-group kill may surface as 143 (SIGTERM) instead of the synthetic 142.
if [[ ( "$code" -eq 142 || "$code" -eq 143 ) && "$allow_timeout" == "true" ]]; then
  echo "intentional timeout after $timeout_seconds seconds; asserting output"
  code=0
fi
finished="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

# A zero exit code is not enough: a connector may fail during class loading and
# still let the client report FINISHED. Source and transform smoke cases must
# produce at least one output row.
output_rows=0
if [[ $code -eq 0 && ( "$case_id" == SR-* || "$case_id" == TR-* ) ]]; then
  # Streaming sources may flush the final file just after the process is stopped.
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    output_rows="$(python3 - "$case_out" <<'PY'
import glob
import sys
files = glob.glob(sys.argv[1] + "/**/part-*.json", recursive=True)
rows = 0
for path in files:
    with open(path, "r", encoding="utf-8") as handle:
        rows += sum(1 for _ in handle)
print(rows)
PY
)"
    [[ "$output_rows" -gt 0 ]] && break
    sleep 1
  done
  if [[ "$output_rows" -le 0 ]]; then
    echo "assertion failed: expected output rows, found 0 in $case_out" | tee -a "$log" >&2
    code=1
  fi
fi

if [[ $code -eq 0 && "$case_id" == SK-* ]]; then
  if ! "$ROOT/scripts/verify/verify-case.sh" "$case_id" >> "$log" 2>&1; then
    echo "target assertion failed for $case_id; see ${log#$ROOT/}" >&2
    code=1
  fi
fi

cat > "$case_dir/result.json" <<JSON
{
  "caseId": "$case_id",
  "dataset": "$dataset",
  "template": "${job_template#$ROOT/}",
  "exitCode": $code,
  "outputRows": $output_rows,
  "startedAt": "$started",
  "finishedAt": "$finished",
  "log": "${log#$ROOT/}"
}
JSON

echo "exit:     $code"
echo "log:      ${log#$ROOT/}"
if [[ $code -ne 0 ]]; then
  tail -40 "$log" >&2
fi
exit "$code"
