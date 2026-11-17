#!/usr/bin/env bash
#
# Load the generated CSV files into PostgreSQL through the containerized psql.
# The data/generated directory is mounted at /data inside the postgres container.

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

PG_USER="${PG_USER:-pudding}"
PG_DB="${PG_DB:-source_db}"
DATA_DIR="${PUDDING_DATA_DIR:-$ROOT/data/generated}"
DATA_CONTAINER_DIR="${PUDDING_DATA_CONTAINER_DIR:-/data}"
TABLES="${PUDDING_TABLES:-orders customers payments}"

compose() {
  docker compose -f "$COMPOSE_FILE" --profile relational "$@"
}

if ! compose ps --status running postgres | grep -q postgres; then
  echo "postgres is not running; start it with scripts/services/up.sh relational" >&2
  exit 1
fi

for table in $TABLES; do
  if [[ ! -s "$DATA_DIR/$table.csv" ]]; then
    echo "missing dataset: $DATA_DIR/$table.csv (run 01_generate_tables.py first)" >&2
    exit 1
  fi
done

echo "loading PostgreSQL database=$PG_DB tables='$TABLES' from $DATA_DIR"

for table in $TABLES; do
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -U "$PG_USER" -d "$PG_DB" <<SQL
TRUNCATE TABLE $table;
\copy $table FROM '$DATA_CONTAINER_DIR/$table.csv' WITH (FORMAT csv, HEADER true)
SQL
done

compose exec -T postgres psql -v ON_ERROR_STOP=1 -U "$PG_USER" -d "$PG_DB" <<'SQL'
ANALYZE orders;
ANALYZE customers;
ANALYZE payments;
SQL

echo "loaded row counts:"
compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -At -c \
  "SELECT 'orders', count(*) FROM orders UNION ALL SELECT 'customers', count(*) FROM customers UNION ALL SELECT 'payments', count(*) FROM payments;"

echo "relation sizes:"
compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -At -c \
  "SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) FROM pg_class WHERE relname IN ('orders','customers','payments') ORDER BY relname;"
