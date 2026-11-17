#!/usr/bin/env bash
#
# Target-side assertions for one sink smoke case.
#
# Usage: scripts/verify/verify-case.sh SK-03
# The relevant service must be running. Exits non-zero on a mismatch.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
case_id="${1:-}"

if [[ -z "$case_id" ]]; then
  echo "usage: $0 <sink-case-id>" >&2
  exit 2
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

PG_USER="${PG_USER:-pudding}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-pudding}"
ES_PORT="${ES_PORT:-19200}"
EXPECTED=100000

fail() {
  echo "ASSERT FAIL: $*" >&2
  exit 1
}

compose() {
  docker compose -f "$COMPOSE_FILE" --profile "$1" "${@:2}"
}

case "$case_id" in
  SK-01)
    grep -q "order_id" "$ROOT/results/runs/SK-01/submit.log" || fail "console output missing"
    ;;
  SK-02)
    rows=$(python3 - "$ROOT/results/runs/SK-02/output/result" <<'PY'
import glob, sys
print(sum(sum(1 for _ in open(f)) for f in glob.glob(sys.argv[1] + "/part-*.json")))
PY
)
    [[ "$rows" -eq "$EXPECTED" ]] || fail "file sink rows $rows != $EXPECTED"
    ;;
  SK-03)
    rows=$(compose relational exec -T postgres psql -U "$PG_USER" -d target_db -At -c \
      "select count(*) from orders;" | tr -d '[:space:]')
    [[ "$rows" -eq "$EXPECTED" ]] || fail "postgres target rows $rows != $EXPECTED"
    ;;
  SK-04)
    rows=$(compose relational exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql \
      mysql -uroot -N -B target_db -e "select count(*) from orders;" | tr -d '[:space:]')
    [[ "$rows" -eq "$EXPECTED" ]] || fail "mysql target rows $rows != $EXPECTED"
    ;;
  SK-05)
    rows=$(compose relational exec -T mongo mongosh --port 27018 --quiet --eval \
      'db.getSiblingDB("source_db").sk05_orders.countDocuments()' | tr -d '[:space:]')
    [[ "$rows" -eq "$EXPECTED" ]] || fail "mongo target rows $rows != $EXPECTED"
    ;;
  SK-06)
    rows=$(curl -s --max-time 10 "http://localhost:${ES_PORT}/sk06_orders/_count" \
      | jq -r '.count // 0')
    [[ "$rows" -eq "$EXPECTED" ]] || fail "es target rows $rows != $EXPECTED"
    ;;
  SK-07)
    rows=$("$ROOT/.venv/bin/python" - <<'PY'
from confluent_kafka import Consumer, TopicPartition
c = Consumer({"bootstrap.servers": "localhost:19092", "group.id": "pudding-verify"})
try:
    low, high = c.get_watermark_offsets(
        TopicPartition("orders-sink-smoke", 0), timeout=10, cached=False)
    print(high - low)
finally:
    c.close()
PY
)
    [[ "$rows" -eq "$EXPECTED" ]] || fail "kafka target rows $rows != $EXPECTED"
    ;;
  SK-08)
    export JAVA_HOME="${PUDDING_JAVA_HOME:-/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home}"
    export PATH="$JAVA_HOME/bin:$PATH"
    rows=$("$SPARK_HOME/bin/spark-sql" --master 'local[2]' \
      --conf spark.sql.catalogImplementation=hive \
      --conf "spark.sql.warehouse.dir=$ROOT/data/hive/warehouse" \
      --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$ROOT/data/hive/metastore_db;create=true" \
      -e "select count(*) from default.pudding_orders_hive" 2>/dev/null \
      | awk '/^[0-9]+$/{value=$0} END{print value}')
    [[ "$rows" -eq "$EXPECTED" ]] || fail "hive target rows '$rows' != $EXPECTED"
    ;;
  SK-09)
    count=$(find "$ROOT/results/runs/SK-09/output/hudi/orders" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    [[ "$count" -gt 0 ]] || fail "hudi output has no parquet files"
    "$ROOT/scripts/verify/table-check.sh" hudi \
      "$ROOT/results/runs/SK-09/output/hudi/orders" --count "$EXPECTED"
    ;;
  SK-10)
    count=$(find "$ROOT/data/generated/lakehouse/paimon/pudding_test.db/sk10_orders" \
      -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    [[ "$count" -gt 0 ]] || fail "paimon output has no parquet files"
    "$ROOT/scripts/verify/table-check.sh" paimon \
      "$ROOT/data/generated/lakehouse/paimon/pudding_test.db/sk10_orders" \
      --count "$EXPECTED"
    ;;
  SK-11)
    count=$(find "$ROOT/data/generated/lakehouse/iceberg/pudding_test/sk11_orders/data" \
      -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    [[ "$count" -gt 0 ]] || fail "iceberg output has no parquet files"
    "$ROOT/scripts/verify/table-check.sh" iceberg \
      "$ROOT/data/generated/lakehouse/iceberg/pudding_test/sk11_orders" \
      --count "$EXPECTED"
    ;;
  *)
    echo "no verifier for $case_id"
    exit 2
    ;;
esac

echo "$case_id target assertion passed"
