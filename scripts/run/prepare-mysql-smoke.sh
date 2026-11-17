#!/usr/bin/env bash
#
# Create a 10,000-row source_db.orders_smoke table for CDC cases that need an
# initial snapshot. The scale orders table has 10M rows and would make a smoke
# snapshot unnecessarily slow.
#
# Usage: scripts/run/prepare-mysql-smoke.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" --profile relational exec -T mysql \
  mysql -upudding -ppudding source_db -e "
    DROP TABLE IF EXISTS orders_smoke;
    CREATE TABLE orders_smoke LIKE orders;
    INSERT INTO orders_smoke SELECT * FROM orders WHERE order_id <= 10000;
    SELECT COUNT(*) AS orders_smoke_rows FROM orders_smoke;
  " 2>/dev/null
