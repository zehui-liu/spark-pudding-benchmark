#!/usr/bin/env bash
#
# Reset loaded test data without deleting the generated CSV/JSONL files.
# This is destructive: pass --yes to run it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"

if [[ "${1:-}" != "--yes" ]]; then
  echo "usage: scripts/services/reset-data.sh --yes" >&2
  exit 2
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

PG_USER="${PG_USER:-pudding}"
PG_DB="${PG_DB:-source_db}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-pudding}"
MYSQL_DB="${MYSQL_DB:-source_db}"
ES_PORT="${ES_PORT:-19200}"
KAFKA_TOPICS="${KAFKA_TOPICS:-orders-json orders-avro orders-protobuf}"

compose() {
  docker compose -f "$COMPOSE_FILE" --profile "$1" "${@:2}"
}

echo "truncating PostgreSQL and MySQL tables"
compose relational exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -c \
  "TRUNCATE TABLE orders, customers, payments;"
compose relational exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql mysql -uroot "$MYSQL_DB" -e \
  "TRUNCATE TABLE orders; TRUNCATE TABLE customers; TRUNCATE TABLE payments;"

echo "dropping MongoDB collections"
compose relational exec -T mongo mongosh --port 27018 --quiet --eval '
  const db = db.getSiblingDB("source_db");
  ["orders", "customers", "payments"].forEach(function (name) {
    db.getCollection(name).drop();
  });
'

echo "deleting Elasticsearch indices"
for index in orders customers payments; do
  curl -fsS -X DELETE "http://localhost:${ES_PORT}/${index}" >/dev/null 2>&1 || true
done

echo "deleting Kafka topics"
for topic in $KAFKA_TOPICS; do
  compose streaming exec -T kafka bash -lc \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic '$topic' --if-exists" \
    >/dev/null 2>&1 || true
done

echo "removing lakehouse baseline data"
rm -rf "$ROOT/data/generated/lakehouse"

echo "reset complete; generated CSV/JSONL files were kept"
