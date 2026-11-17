#!/usr/bin/env bash
#
# Prepare the MinIO, PostgreSQL and input assets used by the object-store cases.
#
# Usage:
#   scripts/run/prepare-object-store.sh --reset
#
# --reset removes the S3A checkpoint/state/warehouse prefixes, recreates the
# Iceberg JDBC catalog database and reseeds the OS-01 PostgreSQL tables. Without
# it the script only repairs the MinIO users, secret file and local input data.
#
# The secret file is written with mode 0600. Job configurations carry only
# ${secret:file:...#key} references; the values are resolved on the driver.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$ROOT/config/docker/docker-compose.yml"
ENV_FILE="$ROOT/config/docker/.env"
STATE_DIR="$ROOT/results/state/object-store"
SECRET_FILE="$STATE_DIR/secrets.properties"
HADOOP_CONF="$STATE_DIR/hadoop-conf"

reset=false
if [[ "${1:-}" == "--reset" ]]; then
  reset=true
elif [[ $# -gt 0 ]]; then
  echo "usage: $0 [--reset]" >&2
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Test-only credentials. They are scoped by the MinIO policies below so the
# checkpoint/state credentials cannot read the Iceberg warehouse and the
# Iceberg credentials cannot read checkpoints.
STATE_ACCESS="pudding-state"
STATE_SECRET="pudding-state-secret"
LAKE_ACCESS="pudding-lake"
LAKE_SECRET="pudding-lake-secret"

umask 077
mkdir -p "$STATE_DIR" "$HADOOP_CONF"

mc_run() {
  docker compose -f "$COMPOSE_FILE" --profile infra run --rm -T --no-deps \
    -e STATE_ACCESS="$STATE_ACCESS" -e STATE_SECRET="$STATE_SECRET" \
    -e LAKE_ACCESS="$LAKE_ACCESS" -e LAKE_SECRET="$LAKE_SECRET" \
    --entrypoint /bin/sh minio-init -c "$1"
}

if [[ "$reset" == "true" ]]; then
  echo "resetting local outputs and object-store prefixes"
  rm -rf "$ROOT/results/state/os-01" "$ROOT/results/state/os-04" \
    "$ROOT/data/generated/os01"
  mc_run '
    set -e
    mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
    for prefix in checkpoints/os-01 state/os-01 warehouse/os-02 warehouse/os-02-denied; do
      mc rm -r --force "local/pudding/$prefix" >/dev/null 2>&1 || true
    done
  '
fi

echo "configuring scoped MinIO users"
mc_run '
  set -e
  mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

  cat > /tmp/state-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": ["arn:aws:s3:::pudding"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::pudding"],
     "Condition": {"StringLike": {"s3:prefix": ["checkpoints/*", "state/*"]}}},
    {"Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": ["arn:aws:s3:::pudding/checkpoints/*", "arn:aws:s3:::pudding/state/*"]}
  ]
}
JSON

  cat > /tmp/lake-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": ["arn:aws:s3:::pudding"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::pudding"],
     "Condition": {"StringLike": {"s3:prefix": ["warehouse/os-02/*", "warehouse/os-02-denied/*"]}}},
    {"Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": ["arn:aws:s3:::pudding/warehouse/os-02/*",
                  "arn:aws:s3:::pudding/warehouse/os-02-denied/*"]}
  ]
}
JSON

  mc admin policy create local pudding-os-state /tmp/state-policy.json >/dev/null 2>&1 || true
  mc admin policy create local pudding-os-lake /tmp/lake-policy.json >/dev/null 2>&1 || true
  mc admin user add local "$STATE_ACCESS" "$STATE_SECRET" >/dev/null 2>&1 || true
  mc admin user add local "$LAKE_ACCESS" "$LAKE_SECRET" >/dev/null 2>&1 || true
  mc admin policy attach local pudding-os-state --user "$STATE_ACCESS" >/dev/null 2>&1 || true
  mc admin policy attach local pudding-os-lake --user "$LAKE_ACCESS" >/dev/null 2>&1 || true
' 2>&1 | tail -3

cat > "$SECRET_FILE" <<EOF
# Test-only credentials for the object-store cases. Mode 0600.
state-access=$STATE_ACCESS
state-secret=$STATE_SECRET
lake-access=$LAKE_ACCESS
lake-secret=$LAKE_SECRET
catalog-user=${PG_USER:-pudding}
catalog-password=${PG_PASSWORD:-pudding}
EOF
chmod 600 "$SECRET_FILE"

cat > "$HADOOP_CONF/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.s3a.endpoint</name><value>http://localhost:${MINIO_PORT:-19000}</value></property>
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
  <property><name>fs.s3a.aws.credentials.provider</name><value>com.amazonaws.auth.EnvironmentVariableCredentialsProvider</value></property>
</configuration>
EOF
chmod 600 "$HADOOP_CONF/core-site.xml"

write_checkpoint_conf() {
  local file="$1" access="$2" secret="$3"
  cat > "$file" <<EOF
fs.s3a.endpoint=http://localhost:${MINIO_PORT:-19000}
fs.s3a.path.style.access=true
fs.s3a.connection.ssl.enabled=false
fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
fs.s3a.access.key=$access
fs.s3a.secret.key=$secret
EOF
  chmod 600 "$file"
}
write_checkpoint_conf "$STATE_DIR/checkpoint-state.properties" "$STATE_ACCESS" "$STATE_SECRET"
write_checkpoint_conf "$STATE_DIR/checkpoint-lake.properties" "$LAKE_ACCESS" "$LAKE_SECRET"

cat > "$STATE_DIR/state-s3a.properties" <<EOF
backend=filesystem
statePath=s3a://pudding/state/os-01/incremental
allowBestEffortLock=true
EOF
chmod 600 "$STATE_DIR/state-s3a.properties"

cat > "$STATE_DIR/spark-catalog.properties" <<EOF
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg_s3=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg_s3.type=jdbc
spark.sql.catalog.iceberg_s3.uri=jdbc:postgresql://localhost:${PG_PORT:-15432}/iceberg_catalog
spark.sql.catalog.iceberg_s3.jdbc.user=${PG_USER:-pudding}
spark.sql.catalog.iceberg_s3.jdbc.password=${PG_PASSWORD:-pudding}
spark.sql.catalog.iceberg_s3.jdbc.schema-version=V1
spark.sql.catalog.iceberg_s3.warehouse=s3://pudding/warehouse/os-02
spark.sql.catalog.iceberg_s3.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg_s3.s3.endpoint=http://localhost:${MINIO_PORT:-19000}
spark.sql.catalog.iceberg_s3.s3.path-style-access=true
spark.sql.catalog.iceberg_s3.s3.access-key-id=$LAKE_ACCESS
spark.sql.catalog.iceberg_s3.s3.secret-access-key=$LAKE_SECRET
EOF
chmod 600 "$STATE_DIR/spark-catalog.properties"

psql_source() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d source_db -v ON_ERROR_STOP=1 -At -c "$1"
}

psql_target() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d target_db -v ON_ERROR_STOP=1 -At -c "$1"
}

psql_admin() {
  docker compose -f "$COMPOSE_FILE" --profile relational exec -T postgres \
    psql -U "${PG_USER:-pudding}" -d postgres -v ON_ERROR_STOP=1 -At -c "$1"
}

if [[ "$reset" == "true" ]]; then
  echo "recreating iceberg_catalog"
  psql_admin "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'iceberg_catalog' AND pid <> pg_backend_pid();" >/dev/null || true
  psql_admin "DROP DATABASE IF EXISTS iceberg_catalog;" >/dev/null
  psql_admin "CREATE DATABASE iceberg_catalog;" >/dev/null

  echo "seeding OS-01 source and target tables"
  psql_source "
    DROP TABLE IF EXISTS os01_orders;
    CREATE TABLE os01_orders (
      order_id bigint PRIMARY KEY,
      customer_id bigint NOT NULL,
      amount numeric(12, 2) NOT NULL,
      status varchar(16) NOT NULL,
      city varchar(32) NOT NULL,
      order_time timestamp NOT NULL,
      updated_at timestamp NOT NULL,
      payload varchar(256) NOT NULL
    );
    INSERT INTO os01_orders
    SELECT g, g % 1000, (g % 500)::numeric(12, 2), 'PAID', 'shanghai',
           timestamp '2026-01-01 00:00:00' + (g || ' seconds')::interval,
           timestamp '2026-01-01 00:00:00' + (g || ' seconds')::interval,
           'os01-' || g
    FROM generate_series(1, 100) AS g;
  " >/dev/null
  psql_target "
    DROP TABLE IF EXISTS os01_orders;
    CREATE TABLE os01_orders (
      LIKE public.orders INCLUDING ALL,
      PRIMARY KEY (order_id)
    );
  " >/dev/null
fi

# A streaming file source needs one immutable batch per start. batch1 is read
# by the first run; the runner adds batch2 before the third run.
mkdir -p "$ROOT/data/generated/os01"
if [[ ! -f "$ROOT/data/generated/os01/batch1.csv" ]]; then
  {
    head -1 "$ROOT/data/seed/e2e03_orders.csv"
    tail -n +2 "$ROOT/data/seed/e2e03_orders.csv" | head -10
  } > "$ROOT/data/generated/os01/batch1.csv"
fi

echo "object-store assets ready: ${STATE_DIR#$ROOT/}"
echo "  secret file:      ${SECRET_FILE#$ROOT/} (0600)"
echo "  hadoop conf:      ${HADOOP_CONF#$ROOT/} (0600)"
echo "  s3a state target: ${STATE_DIR#$ROOT/}/state-s3a.properties"
