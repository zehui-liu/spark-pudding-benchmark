#!/usr/bin/env bash
#
# E2E-06 scale variant: 10M MySQL initial snapshot through the native CDC
# source into Iceberg. Uses a separate state/checkpoint directory from the Hudi
# variant so the two benchmarks do not overwrite each other.
#
# Usage: scripts/run/run-e2e06-iceberg-scale.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CASE="E2E-06-iceberg-mysql-cdc-scale"
TABLE_DIR="$ROOT/results/state/e2e-06-iceberg-scale/iceberg/pudding_test/e2e06_scale_orders"

set -a
# shellcheck disable=SC1090
source "$ROOT/config/docker/.env"
set +a
if [[ "${PUDDING_USE_HADOOP_CONF:-0}" != "1" ]]; then
  unset HADOOP_CONF_DIR
fi

# Query the source count with the same client used by the other E2E runners.
expected="$(docker compose -f "$ROOT/config/docker/docker-compose.yml" --profile relational \
  exec -T mysql mysql -upudding -ppudding -N -e 'SELECT COUNT(*) FROM source_db.orders;' 2>/dev/null | tail -1)"
[[ -n "$expected" ]] || { echo "could not read MySQL source count" >&2; exit 1; }

if [[ ! -d "$TABLE_DIR" ]]; then
  env -u HADOOP_CONF_DIR "$ROOT/scripts/run/create-iceberg-table.sh" \
    "$ROOT/results/state/e2e-06-iceberg-scale/iceberg" \
    pudding_test e2e06_scale_orders cdc
fi
rm -rf "$ROOT/results/state/e2e-06-iceberg-scale/checkpoint"
echo "== E2E-06-iceberg-scale: $expected-row MySQL initial snapshot into Iceberg =="
"$ROOT/scripts/run/run-case.sh" "$CASE" --dataset scale \
  --timeout "${E2E06_ICEBERG_SCALE_TIMEOUT:-4800}" --allow-timeout

env -u HADOOP_CONF_DIR "$ROOT/scripts/verify/table-check.sh" iceberg \
  "$TABLE_DIR" --count "$expected"
echo "E2E-06-iceberg-scale PASS ($expected-row CDC snapshot into Iceberg)"
