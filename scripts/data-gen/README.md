# Data generation and loading

English | [简体中文](README.zh-CN.md)

All scripts resolve the testbed root from their own location, so they can be
run from anywhere:

```shell
cd /Users/zhliu/code/own-code/spark-pudding-benchmark
```

## Prerequisites

- Docker Compose v2 and the testbed images;
- arm64 JDK 11 (or 17), Spark 3.4.0;
- an assembled spark-pudding distribution;
- Python 3.11 with the loader dependencies:

```shell
scripts/data-gen/setup-python.sh
```

The generated data is written to `data/generated/`; smoke data is roughly
200 MB, scale data is at least 1 GiB per primary table.

## Generate

```shell
# Fast iteration: 100,000 rows per table
python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/generated --formats csv,jsonl --force

# Scale gate: 10,000,000 rows per table, >= 1 GiB per table
python3 scripts/data-gen/01_generate_tables.py \
  --profile scale --out data/generated --formats csv,jsonl --force

# Incremental change set for sync/CDC cases
python3 scripts/data-gen/02_generate_delta.py \
  --base-rows 10000000 --out data/generated --force
```

`manifest.json` records row counts, file sizes, SHA-256 values and the profile.
The scale command prints a warning if any output is below 1 GiB.

## Load relational data

Start the profile first:

```shell
scripts/services/up.sh relational
```

Then load:

```shell
scripts/data-gen/03_load_postgres.sh
scripts/data-gen/04_load_mysql.sh
.venv/bin/python scripts/data-gen/05_load_mongo.py \
  --data-dir data/generated --tables orders,customers,payments
```

The PostgreSQL and MySQL scripts truncate their target tables before loading and
print row counts and table sizes. `PUDDING_DATA_DIR` selects the host directory;
`PUDDING_DATA_CONTAINER_DIR` selects the matching container mount (`/data` for
`data/generated`, `/data-seed` for `data/seed`).

## Load Kafka

```shell
scripts/services/up.sh streaming

# JSON: no Schema Registry needed
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/generated/orders.jsonl --format json

# Avro / Protobuf: registers the subject in Schema Registry
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/generated/orders.jsonl --format avro
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/generated/orders.jsonl --format protobuf
```

Use `--limit 5000` for a quick check. `--rate` throttles messages per second.

## Load Elasticsearch

```shell
scripts/services/up.sh search
.venv/bin/python scripts/data-gen/07_load_es.py \
  --data-dir data/generated --tables orders,customers,payments
```

For the 10M-row scale data set, loading one table per process uses the indexing
threads more effectively:

```shell
for table in orders customers payments; do
  .venv/bin/python scripts/data-gen/07_load_es.py \
    --data-dir data/generated --tables "$table" --batch-size 50000 \
    > "/tmp/pudding-es-$table.log" 2>&1 &
done
wait
```

The script recreates the indices and uses the business id as `_id`.

## Build lakehouse baselines

The wrapper adds the matching runtime jar and Spark settings:

```shell
scripts/data-gen/08_build_lakehouse.sh --format hudi
scripts/data-gen/08_build_lakehouse.sh --format paimon
scripts/data-gen/08_build_lakehouse.sh --format iceberg
```

Use `--tables orders` or `--limit 1000` for a quick run. Hudi writes a path
table; Paimon and Iceberg use local catalogs under `data/generated/lakehouse`.

## Verify the 1 GiB gate

Because the Docker VM is small, check each profile while it is running and merge
the reports afterwards:

```shell
# relational is running
python3 scripts/data-gen/09_verify_sizes.py \
  --systems postgres,mysql,mongo \
  --out results/data-scale/size-relational.json

# streaming is running
python3 scripts/data-gen/09_verify_sizes.py \
  --systems kafka \
  --out results/data-scale/size-streaming.json

# search is running
python3 scripts/data-gen/09_verify_sizes.py \
  --systems es \
  --out results/data-scale/size-search.json

# lakehouse files are local
python3 scripts/data-gen/09_verify_sizes.py \
  --systems lakehouse \
  --out results/data-scale/size-lakehouse.json

# CSV-only quick check
python3 scripts/data-gen/09_verify_sizes.py \
  --systems csv --out results/data-scale/size-csv.json

# Merge into the final gate report
python3 scripts/data-gen/10_merge_size_reports.py \
  results/data-scale/size-*.json
```

The verifier exits non-zero when a loaded table is below 1 GiB. The merge script
also fails when a requested system is missing from the reports. The current 1 GiB
run passed; see [results/data-scale/README.md](../../results/data-scale/README.md).

## Run profiles one at a time

The compose files support concurrent profiles, but the current Docker VM has
only ~4 GiB RAM. Recommended sequence:

1. `relational` → load PostgreSQL/MySQL/MongoDB;
2. `streaming` → Kafka and Schema Registry;
3. `search` → Elasticsearch;
4. `infra` → MinIO/Redis/etcd;
5. `hdfs` (optional).

Stop a profile with `scripts/services/down.sh <profile>` before starting the
next memory-heavy one. `down.sh` stops only that profile; data volumes remain.
