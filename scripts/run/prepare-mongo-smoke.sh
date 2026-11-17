#!/usr/bin/env bash
#
# Create a 10,000-document MongoDB collection for SR-04. The production-scale
# collection has 10M documents and would make a source smoke unnecessarily slow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" --profile relational exec -T mongo \
  mongosh --port 27018 --quiet --eval '
    const source = db.getSiblingDB("source_db");
    source.orders.aggregate([{$limit: 10000}, {$out: "orders_smoke"}]).toArray();
    print("orders_smoke documents: " + source.orders_smoke.countDocuments());
  '

# $out renames a temporary collection into place. A change stream opened at the
# current operation time can still observe that rename when the cluster time has
# not advanced past it, so let the timestamp advance before a CDC job starts.
sleep 2
