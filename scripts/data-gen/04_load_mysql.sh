#!/usr/bin/env bash
#
# Load the generated CSV files into MySQL with server-side LOAD DATA.
# The mysql service disables the secure-file-priv directory check and mounts
# data/generated read-only at /data, so the server can read the files directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-pudding}"
MYSQL_DB="${MYSQL_DB:-source_db}"
DATA_DIR="${PUDDING_DATA_DIR:-$ROOT/data/generated}"
DATA_CONTAINER_DIR="${PUDDING_DATA_CONTAINER_DIR:-/data}"
TABLES="${PUDDING_TABLES:-orders customers payments}"

compose() {
  docker compose -f "$COMPOSE_FILE" --profile relational "$@"
}

if ! compose ps --status running mysql | grep -q mysql; then
  echo "mysql is not running; start it with scripts/services/up.sh relational" >&2
  exit 1
fi

for table in $TABLES; do
  if [[ ! -s "$DATA_DIR/$table.csv" ]]; then
    echo "missing dataset: $DATA_DIR/$table.csv (run 01_generate_tables.py first)" >&2
    exit 1
  fi
done

echo "loading MySQL database=$MYSQL_DB tables='$TABLES' from $DATA_DIR"

for table in $TABLES; do
  compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot "$MYSQL_DB" <<SQL
TRUNCATE TABLE $table;
LOAD DATA INFILE '$DATA_CONTAINER_DIR/$table.csv'
INTO TABLE $table
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
SQL
done

compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot "$MYSQL_DB" <<'SQL'
ANALYZE TABLE orders;
ANALYZE TABLE customers;
ANALYZE TABLE payments;
SQL

echo "loaded row counts:"
compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot -N -B "$MYSQL_DB" -e \
  "SELECT 'orders', COUNT(*) FROM orders UNION ALL SELECT 'customers', COUNT(*) FROM customers UNION ALL SELECT 'payments', COUNT(*) FROM payments;"

echo "table sizes (data + index):"
compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot -N -B "$MYSQL_DB" -e \
  "SELECT table_name, ROUND((data_length + index_length) / 1024 / 1024 / 1024, 2) AS size_gib FROM information_schema.tables WHERE table_schema = '$MYSQL_DB' AND table_name IN ('orders','customers','payments') ORDER BY table_name;"
