#!/usr/bin/env bash
#
# Stop one profile, or all profiles, without deleting data volumes by default.
# A named profile stops only its services. "all" removes the whole compose
# project; --volumes removes its data volumes as well.
#
# Usage:
#   scripts/services/down.sh relational
#   scripts/services/down.sh all --volumes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"

delete_volumes=false
profile=""
for argument in "$@"; do
  case "$argument" in
    --volumes|-v) delete_volumes=true ;;
    *) profile="$argument" ;;
  esac
done
profile="${profile:-relational}"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

if [[ "$profile" == "all" ]]; then
  if [[ "$delete_volumes" == "true" ]]; then
    compose down -v
  else
    compose down
  fi
  exit 0
fi

case "$profile" in
  relational) services=(postgres mysql mongo) ;;
  streaming) services=(kafka schema-registry) ;;
  search) services=(elasticsearch) ;;
  infra) services=(minio minio-init redis etcd) ;;
  hdfs) services=(namenode datanode) ;;
  *)
    echo "unknown profile: $profile" >&2
    exit 2
    ;;
esac

echo "stopping profile $profile: ${services[*]}"
# "docker compose stop" accepts service names and does not touch other profiles.
compose stop "${services[@]}"

if [[ "$delete_volumes" == "true" ]]; then
  echo "removing containers and the project's data volumes"
  compose down -v
fi
