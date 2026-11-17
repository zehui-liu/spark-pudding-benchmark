#!/usr/bin/env bash
#
# Run a group of component smoke cases and print a pass/fail summary.
#
# Groups:
#   file        file/Hive/lakehouse sources, sinks and all transforms (no Docker profile)
#   file-basic  file/Hive sources, file/Hive sinks and transforms (no lakehouse data)
#   relational  JDBC/MySQL/MongoDB sources and sinks (relational profile)
#   streaming   Kafka source/sink (streaming profile; run prepare-kafka-smoke.sh first)
#   search      Elasticsearch source/sink (search profile)
#   all         every finite batch smoke case

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN="$SCRIPT_DIR/run-case.sh"

group="${1:-file}"
dataset="${2:-smoke}"

file_cases=(
  SR-01 SR-08 SR-09 SR-10
  SK-01 SK-02 SK-08 SR-07 SK-09 SK-10 SK-11
  TR-01 TR-02 TR-03 TR-04 TR-05 TR-06 TR-07 TR-08 TR-09 TR-10
)
file_basic_cases=(
  SR-01 SK-01 SK-02 SK-08 SR-07
  TR-01 TR-02 TR-03 TR-04 TR-05 TR-06 TR-07 TR-08 TR-09 TR-10
)
relational_cases=(SR-02 SR-03 SR-04 SK-03 SK-04 SK-05)
streaming_cases=(SK-07 SR-06)
search_cases=(SK-06 SR-05)

case "$group" in
  file) cases=("${file_cases[@]}") ;;
  file-basic) cases=("${file_basic_cases[@]}") ;;
  relational) cases=("${relational_cases[@]}") ;;
  streaming) cases=("${streaming_cases[@]}") ;;
  search) cases=("${search_cases[@]}") ;;
  all) cases=("${file_cases[@]}" "${relational_cases[@]}" "${streaming_cases[@]}" "${search_cases[@]}") ;;
  *) echo "unknown group: $group" >&2; exit 2 ;;
esac

summary="$ROOT/results/runs/${group}-summary.txt"
mkdir -p "$ROOT/results/runs"
: > "$summary"
failed=0

for item in "${cases[@]}"; do
  echo "=== $item ==="
  if "$RUN" "$item" --dataset "$dataset"; then
    echo "$item PASS" | tee -a "$summary"
  else
    echo "$item FAIL" | tee -a "$summary"
    failed=$((failed + 1))
  fi
done

echo
echo "summary: ${summary#$ROOT/}"
cat "$summary"
if [[ $failed -gt 0 ]]; then
  echo "$failed case(s) failed" >&2
  exit 1
fi
