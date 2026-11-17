#!/usr/bin/env bash
#
# Start one Docker Compose profile and wait for its services to be healthy.
#
# Usage:
#   scripts/services/up.sh relational
#   scripts/services/up.sh streaming
#   scripts/services/up.sh search
#   scripts/services/up.sh infra
#   scripts/services/up.sh hdfs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$ROOT/config/docker/.env.example" "$ENV_FILE"
  echo "created $ENV_FILE from .env.example"
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

profile="${1:-relational}"

compose() {
  docker compose -f "$COMPOSE_FILE" --profile "$profile" "$@"
}

case "$profile" in
  relational)
    compose up -d --wait postgres mysql mongo
    "$SCRIPT_DIR/init-mongo.sh"
    ;;
  streaming)
    compose up -d --wait kafka schema-registry
    ;;
  search)
    compose up -d --wait elasticsearch
    ;;
  infra)
    compose up -d --wait minio redis etcd
    compose run --rm --no-deps minio-init
    ;;
  hdfs)
    compose up -d --wait namenode datanode
    ;;
  *)
    echo "unknown profile: $profile (expected relational, streaming, search, infra or hdfs)" >&2
    exit 2
    ;;
esac

echo "profile '$profile' is ready"
case "$profile" in
  relational)
    echo "  postgres: localhost:${PG_PORT:-15432}"
    echo "  mysql:    localhost:${MYSQL_PORT:-13306}"
    echo "  mongo:    localhost:${MONGO_PORT:-27018}"
    ;;
  streaming)
    echo "  kafka:             localhost:${KAFKA_PORT:-19092}"
    echo "  schema-registry:   http://localhost:${SCHEMA_REGISTRY_PORT:-18081}"
    ;;
  search)
    echo "  elasticsearch: http://localhost:${ES_PORT:-19200}"
    ;;
  infra)
    echo "  minio:  http://localhost:${MINIO_PORT:-19000} (console ${MINIO_CONSOLE_PORT:-19001})"
    echo "  redis:  localhost:${REDIS_PORT:-16379}"
    echo "  etcd:   localhost:${ETCD_PORT:-12379}"
    ;;
  hdfs)
    echo "  hdfs: hdfs://localhost:${HDFS_NAMENODE_PORT:-18020}"
    ;;
esac
