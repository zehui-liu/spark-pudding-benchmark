#!/usr/bin/env bash
#
# Orchestrate the spark-pudding 1.0.0 benchmark.
#
# Usage:
#   scripts/run/bench-suite.sh preflight
#   scripts/run/bench-suite.sh smoke
#   scripts/run/bench-suite.sh prepare
#   scripts/run/bench-suite.sh scale
#   scripts/run/bench-suite.sh report
#   scripts/run/bench-suite.sh all
#   scripts/run/bench-suite.sh cleanup
#
# Options:
#   --run-id <id>     Use a fixed run id (default: UTC timestamp)
#   --dry-run         Print commands without executing them
#   --force-data      Regenerate scale data even when it already exists
#   --keep-services   Do not stop Docker profiles after `all`
#
# `all` runs preflight, smoke, prepare, scale and report. Each phase can also
# be run separately; `report` reuses the latest suite unless --run-id is given.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSION="$(cat "$ROOT/VERSION")"

COMMAND="${1:-all}"
shift || true

RUN_ID="${BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
EXPLICIT_RUN_ID=0
DRY_RUN=0
FORCE_DATA=0
KEEP_SERVICES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) RUN_ID="$2"; EXPLICIT_RUN_ID=1; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --force-data) FORCE_DATA=1; shift ;;
    --keep-services) KEEP_SERVICES=1; shift ;;
    -h|--help)
      sed -n '2,28p' "$0"
      exit 0
      ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

if [[ "$COMMAND" == "report" && "$EXPLICIT_RUN_ID" == "0" && -z "${BENCH_RUN_ID:-}" ]]; then
  latest="$(ls -dt "$ROOT"/results/bench/suite-* 2>/dev/null | head -1 || true)"
  if [[ -z "$latest" ]]; then
    echo "no benchmark suite found under results/bench/suite-*" >&2
    exit 1
  fi
  RUN_ID="${latest##*/suite-}"
fi

OUT="$ROOT/results/bench/suite-$RUN_ID"
STATUS="$OUT/status.tsv"
LOG="$OUT/run.log"
PHASE="init"

mkdir -p "$OUT"
if [[ ! -f "$STATUS" ]]; then
  printf 'phase\tstatus\tdetail\n' > "$STATUS"
fi
: > "$LOG"

log() {
  printf '[%s] %s\n' "$(date -u +%H:%M:%SZ)" "$*" | tee -a "$LOG"
}

mark() {
  printf '%s\t%s\t%s\n' "$1" "$2" "${3:-}" >> "$STATUS"
}

fail() {
  log "ERROR: $*"
  mark "$PHASE" FAIL "$*"
  exit 1
}

on_error() {
  local code=$?
  trap - ERR
  log "FAILED: $PHASE (exit $code)"
  mark "$PHASE" FAIL "exit $code"
  log "partial results: ${OUT#$ROOT/}"
  log "generate a report with: $0 report --run-id $RUN_ID"
  exit "$code"
}
trap on_error ERR

run() {
  log "+ $*"
  if [[ "$DRY_RUN" == "1" ]]; then
    return 0
  fi
  "$@" 2>&1 | tee -a "$LOG"
}

ensure_env() {
  if [[ ! -f "$ROOT/config/docker/.env" ]]; then
    cp "$ROOT/config/docker/.env.example" "$ROOT/config/docker/.env"
  fi
  set -a
  # shellcheck disable=SC1090
  source "$ROOT/config/docker/.env"
  set +a

  if [[ -f "$ROOT/config/env/env.local.properties" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ROOT/config/env/env.local.properties"
    set +a
  fi

  export SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"
  export PUDDING_HOME="${PUDDING_HOME:-$ROOT/../spark-pudding/build/target/spark-pudding-$VERSION}"
  export PUDDING_BENCH_OUT_ROOT="$OUT/bench"
  mkdir -p "$PUDDING_BENCH_OUT_ROOT"
}

ensure_venv() {
  if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
    run "$ROOT/scripts/data-gen/setup-python.sh"
  fi
}

preflight() {
  PHASE="preflight"
  ensure_env

  run docker info --format 'Docker {{.ServerVersion}}'
  run docker compose version
  run python3 --version
  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
    run "$JAVA_HOME/bin/java" -version
  else
    command -v java >/dev/null 2>&1 || fail "java is not on PATH"
    run java -version
  fi
  [[ -x "$SPARK_HOME/bin/spark-submit" ]] || \
    fail "Spark not found at SPARK_HOME=$SPARK_HOME"
  [[ -x "$PUDDING_HOME/bin/pudding" ]] || \
    fail "spark-pudding distribution not found at PUDDING_HOME=$PUDDING_HOME"
  [[ -f "$PUDDING_HOME/libs/pudding-core-$VERSION.jar" ]] || \
    fail "distribution does not contain pudding-core-$VERSION.jar"

  if [[ -f "$PUDDING_HOME/capabilities.yaml" ]]; then
    dist_version="$(awk '/^productVersion:/ {print $2; exit}' "$PUDDING_HOME/capabilities.yaml")"
    [[ "$dist_version" == "$VERSION" ]] || \
      fail "distribution version '$dist_version' does not match benchmark version '$VERSION'"
  fi

  for file in orders customers payments; do
    [[ -s "$ROOT/data/seed/$file.csv" ]] || fail "missing seed data: data/seed/$file.csv"
  done

  free_gb="$(df -Pk "$ROOT" | awk 'NR==2 {printf "%d", $4/1024/1024}')"
  log "free disk: ${free_gb} GiB"
  if [[ "$free_gb" -lt 30 ]]; then
    log "WARNING: less than 30 GiB free; scale data and lakehouse state may not fit"
  fi
  log "benchmark version: $VERSION; run id: $RUN_ID"
}

smoke() {
  PHASE="smoke"
  ensure_env
  ensure_venv

  run "$ROOT/scripts/run/run-smoke-suite.sh" file-basic smoke

  run "$ROOT/scripts/services/up.sh" relational
  run env PUDDING_DATA_DIR="$ROOT/data/seed" PUDDING_DATA_CONTAINER_DIR=/data-seed \
    "$ROOT/scripts/data-gen/03_load_postgres.sh"
  run env PUDDING_DATA_DIR="$ROOT/data/seed" PUDDING_DATA_CONTAINER_DIR=/data-seed \
    "$ROOT/scripts/data-gen/04_load_mysql.sh"
  run "$ROOT/.venv/bin/python" "$ROOT/scripts/data-gen/05_load_mongo.py" \
    --data-dir "$ROOT/data/seed" --tables orders,customers,payments
  run "$ROOT/scripts/run/prepare-postgres-smoke.sh"
  run "$ROOT/scripts/run/prepare-mysql-smoke.sh"
  run "$ROOT/scripts/run/prepare-mongo-smoke.sh"
  run "$ROOT/scripts/run/run-smoke-suite.sh" relational smoke
  run "$ROOT/scripts/services/down.sh" relational

  run "$ROOT/scripts/services/up.sh" streaming
  run "$ROOT/scripts/run/prepare-kafka-smoke.sh"
  run "$ROOT/scripts/run/run-smoke-suite.sh" streaming smoke
  run "$ROOT/scripts/services/down.sh" streaming

  run "$ROOT/scripts/services/up.sh" search
  run "$ROOT/.venv/bin/python" "$ROOT/scripts/data-gen/07_load_es.py" \
    --data-dir "$ROOT/data/seed" --tables orders,customers,payments
  run "$ROOT/scripts/run/run-smoke-suite.sh" search smoke
  run "$ROOT/scripts/services/down.sh" search
}

prepare() {
  PHASE="prepare"
  ensure_env

  if [[ "$FORCE_DATA" == "1" || ! -s "$ROOT/data/generated/orders.csv" ]]; then
    run python3 "$ROOT/scripts/data-gen/01_generate_tables.py" \
      --profile scale --out "$ROOT/data/generated" --formats csv,jsonl --force
  fi
  run python3 "$ROOT/scripts/data-gen/09_verify_sizes.py" \
    --systems csv --out "$ROOT/results/data-scale/size-csv-suite-$RUN_ID.json"

  run "$ROOT/scripts/services/up.sh" relational
  run env PUDDING_DATA_DIR="$ROOT/data/generated" "$ROOT/scripts/data-gen/03_load_postgres.sh"
  run env PUDDING_DATA_DIR="$ROOT/data/generated" "$ROOT/scripts/data-gen/04_load_mysql.sh"

  if [[ ! -d "$ROOT/data/generated/lakehouse/iceberg/pudding_test/e2e14_orders" ]]; then
    run env -u HADOOP_CONF_DIR "$ROOT/scripts/run/create-iceberg-table.sh" \
      "$ROOT/data/generated/lakehouse/iceberg" pudding_test e2e14_orders
  fi

  run "$ROOT/scripts/services/down.sh" relational
}

require_scale_data() {
  [[ -s "$ROOT/data/generated/orders.csv" ]] || \
    fail "scale data is missing; run: $0 prepare --run-id $RUN_ID"
  [[ -d "$ROOT/data/generated/lakehouse/iceberg/pudding_test/e2e14_orders" ]] || \
    fail "E2E-14 Iceberg table is missing; run: $0 prepare --run-id $RUN_ID"
}

scale() {
  PHASE="scale"
  ensure_env
  ensure_venv
  require_scale_data

  run "$ROOT/scripts/run/bench-case.sh" E2E-01 --rows 10 --result-case E2E-01 -- \
    env E2E01_DATASET=scale "$ROOT/scripts/run/run-e2e.sh" E2E-01

  run "$ROOT/scripts/run/bench-case.sh" E2E-14-iceberg-scale --rows 10000000 \
    --result-case E2E-14-iceberg-full-refresh-scale -- \
    "$ROOT/scripts/run/run-case.sh" E2E-14-iceberg-full-refresh-scale --dataset scale --timeout 3600

  run "$ROOT/scripts/run/bench-case.sh" E2E-14-hudi-scale --rows 10000000 \
    --result-case E2E-14-hudi-full-refresh-scale -- \
    "$ROOT/scripts/run/run-case.sh" E2E-14-hudi-full-refresh-scale --dataset scale --timeout 3600

  run "$ROOT/scripts/run/bench-case.sh" E2E-14-paimon-scale --rows 10000000 \
    --result-case E2E-14-paimon-full-refresh-scale -- \
    "$ROOT/scripts/run/run-case.sh" E2E-14-paimon-full-refresh-scale --dataset scale --timeout 3600

  run "$ROOT/scripts/services/up.sh" relational
  run "$ROOT/scripts/run/bench-case.sh" E2E-04-scale --rows 10000002 \
    --result-case E2E-04-scale -- "$ROOT/scripts/run/run-e2e.sh" E2E-04-scale
  run "$ROOT/scripts/run/bench-case.sh" E2E-06-hudi-scale --rows 10000013 \
    --result-case E2E-06-hudi-mysql-cdc-scale -- "$ROOT/scripts/run/run-e2e.sh" E2E-06-scale
  run "$ROOT/scripts/run/bench-case.sh" E2E-06-iceberg-scale --rows 10000013 \
    --result-case E2E-06-iceberg-mysql-cdc-scale -- "$ROOT/scripts/run/run-e2e06-iceberg-scale.sh"
  run "$ROOT/scripts/services/down.sh" relational

  run "$ROOT/scripts/services/up.sh" streaming
  run "$ROOT/scripts/run/bench-case.sh" E2E-09-scale --rows 10000000 \
    --result-case E2E-09-kafka-to-hudi-scale -- "$ROOT/scripts/run/run-kafka-scale.sh"
  run "$ROOT/scripts/services/down.sh" streaming
}

report() {
  PHASE="report"
  if [[ ! -f "$STATUS" ]]; then
    fail "status file not found: ${STATUS#$ROOT/}"
  fi

  summary="$OUT/metrics-summary.md"
  if compgen -G "$OUT/bench/*/metrics.json" > /dev/null; then
    python3 "$ROOT/scripts/run/bench-summary.py" "$OUT/bench" > "$summary"
  else
    {
      echo "# Benchmark metrics"
      echo
      echo "No metrics were produced for this run."
    } > "$summary"
  fi

  report_file="$OUT/report.md"
  {
    echo "# spark-pudding $VERSION benchmark suite"
    echo
    echo "- Run ID: \`$RUN_ID\`"
    echo "- Benchmark version: \`$VERSION\`"
    echo "- Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- Phase log: \`${LOG#$ROOT/}\`"
    echo
    echo "## Phase status"
    echo
    echo "| Phase | Status | Detail |"
    echo "| --- | --- | --- |"
    awk -F'\t' '
      NR > 1 && $1 != "report" {
        if (!($1 in seen)) { order[++n] = $1; seen[$1] = 1 }
        status[$1] = $2
        detail[$1] = $3
      }
      END {
        for (i = 1; i <= n; i++) {
          printf "| %s | %s | %s |\n", order[i], status[order[i]], detail[order[i]]
        }
      }' "$STATUS"
    echo
    cat "$summary"
    echo
    echo "## Artifacts"
    echo
    echo "- Suite directory: \`${OUT#$ROOT/}\`"
    metrics_dir="${PUDDING_BENCH_OUT_ROOT:-$OUT/bench}"
    echo "- Benchmark metrics: \`${metrics_dir#$ROOT/}\`"
    echo "- Phase status: \`${STATUS#$ROOT/}\`"
    echo "- Full log: \`${LOG#$ROOT/}\`"
  } > "$report_file"

  cp "$report_file" "$ROOT/results/bench/benchmark-report.md"
  log "report: ${report_file#$ROOT/}"
  log "latest report: results/bench/benchmark-report.md"
}

cleanup() {
  PHASE="cleanup"
  for profile in relational streaming search infra hdfs; do
    if [[ -x "$ROOT/scripts/services/down.sh" ]]; then
      run "$ROOT/scripts/services/down.sh" "$profile" || true
    fi
  done
}

run_phase() {
  local phase="$1"
  mark "$phase" RUN "-"
  case "$phase" in
    preflight) preflight ;;
    smoke) smoke ;;
    prepare) prepare ;;
    scale) scale ;;
    report) report ;;
    cleanup) cleanup ;;
    *) fail "unknown phase: $phase" ;;
  esac
  mark "$phase" PASS "-"
}

case "$COMMAND" in
  preflight|smoke|prepare|scale|report|cleanup)
    run_phase "$COMMAND"
    ;;
  all)
    run_phase preflight
    run_phase smoke
    run_phase prepare
    run_phase scale
    run_phase report
    if [[ "$KEEP_SERVICES" == "0" ]]; then
      run_phase cleanup
    fi
    log "completed: ${OUT#$ROOT/}/report.md"
    ;;
  -h|--help)
    sed -n '2,28p' "$0"
    ;;
  *)
    echo "unknown command: $COMMAND" >&2
    sed -n '2,28p' "$0" >&2
    exit 2
    ;;
esac
