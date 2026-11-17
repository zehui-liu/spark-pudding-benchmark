#!/usr/bin/env bash
#
# Run one benchmark case, capture wall time, process/Docker resources and Spark
# event-log metrics, then copy the case result into results/bench/.
#
# Usage:
#   bench-case.sh <case-id> --rows 10000000 [--dataset scale] \
#     [--result-case <result-dir>] -- <command...>
#
# Examples:
#   bench-case.sh E2E-01 --rows 10 --result-case E2E-01 -- \
#     scripts/run/run-e2e.sh E2E-01
#   bench-case.sh E2E-04-scale --rows 10000002 --result-case E2E-04-scale -- \
#     scripts/run/run-e2e.sh E2E-04-scale
#   bench-case.sh E2E-09-scale --rows 10000000 \
#     --result-case E2E-09-kafka-to-hudi-scale -- scripts/run/run-kafka-scale.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"

case_id=""
rows=0
dataset="scale"
result_case=""
command=()

if [[ $# -lt 1 ]]; then
  sed -n '1,20p' "$0" >&2
  exit 2
fi
case_id="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rows) rows="$2"; shift 2 ;;
    --dataset) dataset="$2"; shift 2 ;;
    --result-case) result_case="$2"; shift 2 ;;
    --)
      shift
      command=("$@")
      break
      ;;
    *)
      echo "unknown option: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ${#command[@]} -eq 0 ]]; then
  echo "missing command after --" >&2
  exit 2
fi
if [[ -z "$result_case" ]]; then
  result_case="$case_id"
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
bench_root="${PUDDING_BENCH_OUT_ROOT:-$ROOT/results/bench}"
mkdir -p "$bench_root"
out="$bench_root/${case_id}-${stamp}-$$"
mkdir -p "$out/events"
export PUDDING_BENCH_EVENT_ROOT="$out/events"

rss_csv="$out/rss.csv"
docker_csv="$out/docker.csv"
echo "epochSeconds,rssKb" > "$rss_csv"
echo "epochSeconds,name,memUsage,cpuPercent" > "$docker_csv"

sample_rss() {
  while true; do
    local ts
    ts="$(date +%s)"
    ps -axo rss=,command= |
      awk -v ts="$ts" '
        $0 ~ /(SparkSubmit|org\.pudding|spark-pudding|pudding-client)/ &&
        $0 !~ /(awk|bench-case\.sh|ps -axo)/ { sum += $1 }
        END { print ts "," (sum + 0) }
      ' >> "$rss_csv"
    sleep 5
  done
}

sample_docker() {
  while true; do
    local ts
    ts="$(date +%s)"
    docker stats --no-stream --format '{{.Name}},{{.MemUsage}},{{.CPUPerc}}' 2>/dev/null |
      awk -v ts="$ts" -F',' '{ print ts "," $1 "," $2 "," $3 }' >> "$docker_csv" || true
    sleep 10
  done
}

rss_pid=""
docker_pid=""
cleanup() {
  if [[ -n "$rss_pid" ]]; then kill "$rss_pid" 2>/dev/null || true; fi
  if [[ -n "$docker_pid" ]]; then kill "$docker_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT

sample_rss &
rss_pid=$!
sample_docker &
docker_pid=$!

start_epoch="$(date +%s)"
set +e
"${command[@]}" > "$out/run.log" 2>&1
exit_code=$?
set -e
end_epoch="$(date +%s)"
wall_seconds=$((end_epoch - start_epoch))

cleanup
trap - EXIT

{
  echo "caseId=$case_id"
  echo "resultCase=$result_case"
  echo "dataset=$dataset"
  echo "rows=$rows"
  echo "startedAt=$(date -u -r "$start_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "wallSeconds=$wall_seconds"
  echo "exitCode=$exit_code"
  echo "head=$(git -C "$SIBLING_ROOT/spark-pudding" rev-parse HEAD 2>/dev/null || true)"
  echo "dirtyHash=$(git -C "$SIBLING_ROOT/spark-pudding" status --porcelain |
    shasum -a 256 | awk '{print $1}')"
  echo "distribution=${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
  echo "command=${command[*]}"
} > "$out/meta.env"

if [[ -d "${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}/lib" ]]; then
  shasum -a 256 \
    "${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"/lib/*.jar \
    > "$out/jars.sha256" 2>/dev/null || true
fi

if [[ -d "$ROOT/results/runs/$result_case" ]]; then
  mkdir -p "$out/results"
  cp -R "$ROOT/results/runs/$result_case" "$out/results/"
fi

python3 "$SCRIPT_DIR/parse-spark-events.py" \
  "$out" "$case_id" "$exit_code" "$wall_seconds" "$rows" \
  > "$out/metrics.stdout" 2>&1 || true

echo "benchmark: ${out#$ROOT/}"
cat "$out/metrics.stdout"
exit "$exit_code"
