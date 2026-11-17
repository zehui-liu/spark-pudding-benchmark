#!/usr/bin/env bash
#
# Initialize the single-node MongoDB replica set used by change streams.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"

compose() {
  docker compose -f "$COMPOSE_FILE" --profile relational "$@"
}

for attempt in $(seq 1 60); do
  output="$(compose exec -T mongo mongosh --port 27018 --quiet --eval '
    try {
      const hello = db.hello();
      if (hello.isWritablePrimary) { print("PRIMARY"); }
      else if (hello.setName === "rs0") { print("WAIT"); }
      else {
        try { rs.status(); } catch (e) {
          rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27018"}]});
        }
        print("WAIT");
      }
    } catch (e) {
      print("WAIT");
    }
  ' 2>/dev/null || true)"

  if grep -q PRIMARY <<<"$output"; then
    echo "MongoDB replica set rs0 is primary"
    exit 0
  fi
  sleep 2
done

echo "MongoDB replica set did not become primary within 120s" >&2
exit 1
