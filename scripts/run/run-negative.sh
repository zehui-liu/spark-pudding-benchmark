#!/usr/bin/env bash
#
# Negative cases: bad job configurations must fail before writing data, and the
# error must name the offending key without echoing secret values.
#
# Usage: scripts/run/run-negative.sh [NG-01|NG-02|NG-05|NG-08|all]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CASE_ID="${1:-all}"
TEMPLATES="$ROOT/config/jobs/negative"
COMPOSE_ENV="$ROOT/config/docker/.env"

if [[ -f "$COMPOSE_ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$COMPOSE_ENV"
  set +a
fi
# A cluster fs.defaultFS must not resolve the local file paths used below.
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
export PUDDING_TEST_ROOT="$ROOT"
export PUDDING_TEST_DATA="$ROOT/data/seed"
export PG_URL="jdbc:postgresql://localhost:${PG_PORT:-15432}/source_db"
export PG_TARGET_URL="jdbc:postgresql://localhost:${PG_PORT:-15432}/target_db"
export PG_USER="${PG_USER:-pudding}"
export PG_PASSWORD="${PG_PASSWORD:-pudding}"

prepare_ng06() {
  local input="$ROOT/results/state/ng-06/input"
  rm -rf "$ROOT/results/state/ng-06"
  mkdir -p "$input"
  cat > "$input/rows.csv" <<'CSV'
order_id,customer_id,amount,status,city,order_time,updated_at,payload,op
1,1,10.00,PAID,shanghai,2026-01-01T00:00:01Z,2026-01-01T00:00:01Z,ng06-1,U
2,2,20.00,PAID,beijing,2026-01-01T00:00:02Z,2026-01-01T00:00:02Z,ng06-2,X
CSV
}

fail() {
  echo "ASSERT FAIL: $*" >&2
  exit 1
}

submit_expect_fail() {
  local name="$1"
  local template="$2"
  local expected="$3"
  local forbidden="${4:-}"
  local run_dir="$ROOT/results/runs/$name"
  local rendered="$run_dir/job.yaml"
  local env_file="$run_dir/env.properties"
  local log="$run_dir/submit.log"

  rm -rf "$run_dir"
  mkdir -p "$run_dir/output"
  export CASE_OUT="$run_dir/output"

  envsubst '${SPARK_HOME} ${PUDDING_HOME} ${PUDDING_TEST_ROOT} ${PUDDING_TEST_DATA} ${CASE_OUT} ${PG_URL} ${PG_TARGET_URL} ${PG_USER} ${PG_PASSWORD}' \
    < "$TEMPLATES/$template" > "$rendered"
  {
    echo "JAVA_HOME=$JAVA_HOME"
    echo "SPARK_HOME=$SPARK_HOME"
  } > "$env_file"

  local code=0
  "$PUDDING_HOME/bin/pudding" submit "$env_file" "$rendered" > "$log" 2>&1 || code=$?
  if [[ "$code" -eq 0 ]]; then
    fail "$name unexpectedly succeeded"
  fi
  if ! grep -Fq "$expected" "$log"; then
    tail -20 "$log" >&2
    fail "$name did not report '$expected'"
  fi
  if [[ -n "$forbidden" ]] && grep -Fq "$forbidden" "$log"; then
    fail "$name leaked the sensitive value '$forbidden'"
  fi
  echo "$name PASS (exit $code; asserted '$expected')"
}

run_case() {
  local name="$1"
  case "$name" in
    NG-01) submit_expect_fail "$name" "NG-01-multiple-sinks.yaml" "requires exactly one sink" ;;
    NG-02) submit_expect_fail "$name" "NG-02-unknown-dependency.yaml" "unknown dependencies" ;;
    NG-02-cycle) submit_expect_fail "$name" "NG-02-dependency-cycle.yaml" \
             "transform dependencies contain a cycle" ;;
    NG-03) submit_expect_fail "$name" "NG-03-streaming-hive-source.yaml" \
             "does not support streaming execution" ;;
    NG-04) submit_expect_fail "$name" "NG-04-invalid-output-mode.yaml" \
             "unsupported streaming outputMode" ;;
    NG-05) submit_expect_fail "$name" "NG-05-missing-url.yaml" "can not find param: url" \
             "NG05-SHOULD-NOT-LEAK" ;;
    NG-07) submit_expect_fail "$name" "NG-07-schema-drift.yaml" \
             "missing columns: missing_col" ;;
    NG-08) submit_expect_fail "$name" "NG-08-missing-secret.yaml" \
             "secret environment variable is not set: PUDDING_NG08_MISSING" ;;
    NG-06-invalid-op) submit_expect_fail "$name" "NG-06-invalid-op.yaml" \
             "unsupported cdc operation in op: X" ;;
    NG-06-missing-op-column) submit_expect_fail "$name" "NG-06-missing-op-column.yaml" \
             "cdc input is missing operation column: missing_op" ;;
    NG-06-missing-order) submit_expect_fail "$name" "NG-06-missing-order.yaml" \
             "streaming cdc requires cdcOrderFields or Kafka/Debezium ordering metadata" ;;
    *) fail "unknown negative case: $name" ;;
  esac
}

if [[ "$CASE_ID" == "all" ]]; then
  prepare_ng06
  for name in NG-01 NG-02 NG-02-cycle NG-03 NG-04 NG-05 NG-07 NG-08 \
      NG-06-invalid-op NG-06-missing-op-column NG-06-missing-order; do
    run_case "$name"
  done
  echo "negative suite PASS (11/11)"
else
  [[ "$CASE_ID" == NG-06-* ]] && prepare_ng06
  run_case "$CASE_ID"
fi
