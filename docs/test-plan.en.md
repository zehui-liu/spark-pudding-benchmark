English | [简体中文](test-plan.md)

# spark-pudding Local Full-Chain Test Plan

| Item | Content |
| --- | --- |
| Document version | 1.0 |
| Date written | 2026-11-06 |
| Applicable code | spark-pudding `1.0.0`, branch `v-1.0` |
| Test bed | `spark-pudding-benchmark/` (this directory) |
| Test method | Real distribution package + `bin/pudding submit` + Docker Compose |
| Data scale | All primary test tables (`orders`/`customers`/`payments`) **≥ 1 GiB** per table (uncompressed logical data volume) |

---

## 1. Objectives

This plan is used to verify spark-pudding's complete data chain locally in a single pass, prioritizing the discovery of issues in data correctness, state consistency, resource lifecycle, and error handling.

Coverage targets:

- **13 source types** (11 modules, including 3 CDC sources);
- **11 sink types**;
- **10 transform types**;
- Four execution paths: `batch`, `sync full`, `sync incremental` (append/upsert/cdc), `streaming`;
- checkpoint, incremental cursor state, lock/fencing, failure recovery;
- Object storage (S3A), HDFS, lakehouse table maintenance and capability inventory;
- Positive paths, negative configuration, and fault injection;
- The three primary test tables `orders`, `customers`, `payments`, with a data scale of ≥ 1 GiB per table.

Out of scope for this plan:

- Production clusters, YARN/Kubernetes multi-node performance;
- Security penetration testing;
- Compatible migration with old-version job configurations;
- Long-duration stability/soak testing (can be a separate follow-up plan).

---

## 2. Overall Architecture

```text
宿主机
  ├── Spark 3.4.0 (local[*])
  ├── spark-pudding 发行包
  └── Docker Compose
        ├── relational profile: PostgreSQL / MySQL / MongoDB
        ├── streaming profile:  Kafka / Schema Registry
        ├── search profile:     Elasticsearch
        └── infra profile:      MinIO / Redis / etcd / (可选 HDFS)

每个用例：
  config/jobs/<case>.yaml
    → scripts/run/run-case.sh
    → bin/pudding submit
    → scripts/verify/assert-*.sh
    → results/runs/<case-id>/
```

Design principles:

1. Use the **real distribution package** and the unified CLI rather than only running unit tests;
2. All external dependencies are containerized; the host runs only Spark;
3. Test assets are separated from product code; all scripts, configurations, and data live under `spark-pudding-benchmark/`;
4. Small data is used for fast iteration; 1 GiB data is used for final scale and correctness gates;
5. Every case is repeatable, and on failure the configuration and logs needed for minimal reproduction are retained.

---

## 3. Test Bed Environment

### 3.1 Current Environment

| Item | Value | Notes |
| --- | --- | --- |
| CPU architecture | arm64 | Most images have native arm64 |
| Host memory | 32 GiB | Spark uses host memory |
| Docker memory | ~3.8 GiB | **Recommended to adjust to 8–12 GiB**, otherwise ES/Kafka/DB cannot run simultaneously |
| Docker images/volumes | currently ~9 GB | Recommended Docker disk limit ≥ 100 GB |
| Host disk | ~158 GB available | Three 1 GiB primary tables + multi-system copies estimated at 55–75 GB; recommended to keep ≥100 GB |
| JDK | Zulu 11 / 17 (arm64) | Use 11 or 17 |
| Spark | `/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3` | Consistent with the project baseline |
| Python | 3.11 | Data generation and loading scripts |

### 3.2 Docker Services and Profiles

To avoid the 3.8 GiB Docker memory being fully occupied at once, start and stop services by profile:

| profile | Service | Image | Host port | Purpose |
| --- | --- | --- | --- | --- |
| `relational` | postgres | `postgres:16-alpine` | 15432 | JDBC, PostgreSQL CDC |
| | mysql | `mysql:8.0` | 13306 | MySQL source/sink, binlog CDC |
| | mongo | `mongo:7.0` | 27018 | MongoDB source/sink, change stream |
| `streaming` | kafka | `apache/kafka-native:3.8.0` | 19092 | Kafka source/sink, streaming |
| | schema-registry | `confluentinc/cp-schema-registry:7.4.0` | 18081 | Avro/Protobuf |
| `search` | elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.9.1` | 19200 | ES source/sink |
| `infra` | minio | `quay.io/minio/minio:RELEASE.2023-09-04T19-57-37Z` | 19000/19001 | S3A checkpoint/state, Iceberg S3FileIO |
| | redis | `redis:7-alpine` | 16379 | Redis incremental state |
| | etcd | `quay.io/coreos/etcd:v3.5.9` | 12379 | etcd incremental state |
| `hdfs` (optional) | namenode/datanode | `apache/hadoop:3.3.6` | 18020 | HDFS checkpoint/state |

Ports uniformly use the 1xxxx range to avoid conflicts with existing services on the host. All connection strings are uniformly placed in environment templates under `config/env/`, not hardcoded in job files.

### 3.3 Key Service Parameters

- **PostgreSQL**
  - `wal_level=logical`
  - `max_replication_slots=10`
  - `max_wal_senders=10`
  - After loading completes, create an index on `orders(updated_at)` for use by the incremental cursor.

- **MySQL**
  - `--server-id=1`
  - `--log-bin=mysql-bin`
  - `--binlog-format=ROW`
  - `--binlog-row-image=FULL`
  - Append `useSSL=false&allowPublicKeyRetrieval=true` to the JDBC URL.

- **MongoDB**
  - `--replSet rs0 --bind_ip_all`
  - Initialization:
    ```javascript
    rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27018"}]})
    ```
  - change stream requires a replica set; a single-node replica set is sufficient.

- **Kafka**
  - Dual listeners:
    - Host Spark uses `PLAINTEXT://localhost:19092`
    - Schema Registry inside the container uses `INTERNAL://kafka:29092`
  - Single-node KRaft;
  - `auto.create.topics.enable=true` is for testing only;
  - Set reasonable `retention.ms`/`retention.bytes` for large topics.

- **Elasticsearch**
  - `discovery.type=single-node`
  - `xpack.security.enabled=false`
  - `ES_JAVA_OPTS=-Xms1g -Xmx1g` (1–2g recommended at the scale of three 1 GiB tables)
  - Single shard, 0 replicas, to make it easy to measure primary storage size.

- **Schema Registry**
  - Under arm64, `confluentinc/cp-schema-registry:7.4.0` may run under amd64 emulation;
  - If too slow, start the Confluent Schema Registry process directly with the host JDK;
  - `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=INTERNAL://kafka:29092`.

### 3.4 Directory Conventions

All subsequent test data, scripts, and configurations are placed in the following directories:

| Content | Directory |
| --- | --- |
| Test plan, defect template | `docs/` |
| docker-compose, initialization SQL, service configuration | `config/docker/` |
| env.properties, state properties templates | `config/env/` |
| Per-case job YAML/JSON/HOCON | `config/jobs/` |
| Table DDL, Avro/Protobuf schema, ES mapping | `config/schemas/` |
| Small-scale deterministic seed data | `data/seed/` |
| 1 GiB generated data (rebuildable, not committed) | `data/generated/` |
| Data generation and loading scripts | `scripts/data-gen/` |
| Compose start/stop, health checks, data reset | `scripts/services/` |
| Case executor | `scripts/run/` |
| Data/state/checkpoint assertions | `scripts/verify/` |
| Scale verification reports | `results/data-scale/` |
| Logs and results of each case run | `results/runs/<case-id>/` |
| Temporary files | `tmp/` |

Naming conventions:

- Case IDs: `SR-01`, `SK-03`, `TR-07`, `E2E-05`, `RG-02`, `NG-04`;
- job files: `config/jobs/<case-id>-<short-name>.yaml`;
- result directories: `results/runs/<case-id>/`;
- Each result directory contains at least: `job.yaml`, `env.properties`, `submit.log`, `assert.log`, `result.json`.

---

## 4. Datasets and the 1 GiB Scale Requirement

### 4.1 Measurement Basis

"At least 1G per table" is uniformly calculated as **1 GiB = 1,073,741,824 bytes**, and two measures are recorded at the same time:

1. **Logical data volume**: the total size of uncompressed row/document/message payloads;
2. **Physical storage volume**: database tablespace, collection `storageSize`, ES primary shard store, Kafka log directory, and the actual footprint of lakehouse data files.

Acceptance is based on logical data volume ≥ 1 GiB; physical storage volume is recorded as an observation metric in `results/data-scale/size-report.json`. For columnar lakehouses, if the compressed size is less than 1 GiB, Parquet compression is disabled when writing the scale baseline (`parquet.compression=none`), ensuring the physical data files are also ≥ 1 GiB.

### 4.2 Logical Data Model

Primary test table `orders`:

| Field | Type | Notes |
| --- | --- | --- |
| `order_id` | bigint | Primary key, sequentially generated |
| `customer_id` | bigint | 1–1,000,000, used for join |
| `amount` | decimal(12,2) | Deterministic amount |
| `status` | varchar(16) | `NEW/PAID/SHIPPED/DONE/CANCELLED` cycled |
| `city` | varchar(32) | 20 cities cycled |
| `order_time` | timestamp | Event time, increasing |
| `updated_at` | timestamp | Incremental cursor, increasing |
| `payload` | varchar(256) | 64-character deterministic padding, controls row width |

Primary test table 2 `customers`:

| Field | Type | Notes |
| --- | --- | --- |
| `customer_id` | bigint | Primary key, sequentially generated |
| `name` | varchar(64) | Deterministic name |
| `city` | varchar(32) | Same value domain as the `city` values in orders |
| `segment` | varchar(16) | `VIP/NORMAL/NEW` cycled |
| `updated_at` | timestamp | Incremental cursor |
| `payload` | varchar(256) | 96-character deterministic padding |

Primary test table 3 `payments`:

| Field | Type | Notes |
| --- | --- | --- |
| `payment_id` | bigint | Primary key, sequentially generated |
| `order_id` | bigint | References orders |
| `paid_amount` | decimal(12,2) | Amount |
| `payment_time` | timestamp | Event time |
| `updated_at` | timestamp | Incremental cursor |
| `payload` | varchar(256) | 64-character deterministic padding |

All three primary test tables are generated with **10,000,000 rows and ≥ 1 GiB per table**. Join correctness cases can use `filterExpr` to read only 1,000,000 rows, avoiding a full three-table shuffle on every regression; scale gate cases must run at least one full three-table join.

Incremental dataset (using `orders` as an example):

- `delta_insert`: 50,000 new orders;
- `delta_update`: 200,000 rows updating `amount/status/updated_at`;
- `delta_delete`: 20,000 rows deleted by `order_id`;
- `delta_late`: 10,000 late events, used for watermark testing.

All generation uses a fixed random seed (default `42`) to guarantee reproducibility.

### 4.3 Row Count and Volume Estimates

Baseline row count: **10,000,000 rows for each of the three primary tables**.

| System | Per-table estimate | Total for three primary tables |
| --- | --- | --- |
| CSV | ~1.3–1.4 GB | ~3.9–4.2 GB |
| Parquet (uncompressed) | ~1.1–1.2 GB | ~3.3–3.6 GB |
| PostgreSQL | ~1.5–2.5 GB | ~4.5–7.5 GB |
| MySQL | ~1.5–2.5 GB | ~4.5–7.5 GB |
| MongoDB | ~1.5–2.5 GB | ~4.5–7.5 GB |
| Elasticsearch | ~2–3.5 GB | ~6–10.5 GB |
| Kafka | ~1.3–1.75 GB | Produced separately per topic, ~4–5 GB in total |
| Hudi / Paimon / Iceberg | ~1.1–1.2 GB | ~3.3–3.6 GB per set |

Notes:

- All three primary tables are generated with 10M rows; if a system's physical size already clearly exceeds 1 GiB, it can be reduced to 6M rows, but the logical data volume of that table must still be ≥ 1 GiB;
- 10M rows is a volume estimate; the final measurement in `results/data-scale/size-report.json` prevails;
- If disk space is insufficient, systems can be loaded sequentially on the basis that "the current profile only needs one system", but the final scale report must cover the measurement results of the three tables in all target systems.

### 4.4 Data Generation and Loading

Suggested script breakdown (to be implemented later under `scripts/data-gen/`):

| Script | Purpose |
| --- | --- |
| `01_generate_tables.py` | Stream-generate CSV/JSONL for `orders`, `customers`, `payments` |
| `02_generate_delta.py` | Generate incremental CSV/SQL |
| `03_load_postgres.sh` | Create tables, `COPY` the three tables, create indexes, `ANALYZE` |
| `04_load_mysql.sh` | Create tables, `LOAD DATA` the three tables, create indexes, `ANALYZE TABLE` |
| `05_load_mongo.py` | Batch `insertMany` the three tables, create business indexes |
| `06_produce_kafka.py` | Produce orders JSON/Avro/Protobuf topics |
| `07_load_es.py` | Batch `_bulk` write the three tables and refresh |
| `08_build_lakehouse.sh` / `08_build_lakehouse.py` | Use Spark to generate the Hudi/Paimon/Iceberg three-table baselines |
| `09_verify_sizes.sh` | Summarize logical/physical sizes of the three tables across systems and output a size report |
| `10_merge_size_reports.py` | Merge size reports collected per profile and generate the final gate report |

Generation principles:

- The generator must stream data to files and must not hold 10M rows in memory;
- Generate `data/generated/` once and reuse it for all subsequent loading;
- Data loading scripts must be repeatable: first `DROP/TRUNCATE/DELETE`, then import;
- Generated data is not committed to the repository; only generation scripts and checksums are kept;
- Record `sha256` and row counts to confirm that all systems import the same data.

Example loading commands:

```shell
# PostgreSQL：通过容器内 psql 执行 COPY
docker exec -i pudding-postgres psql -U pudding -d source_db \
  -c "\copy orders FROM '/data/orders.csv' WITH (FORMAT csv, HEADER true)"

# MySQL：把 data/generated 挂载到容器后执行
docker exec -i pudding-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" source_db \
  -e "LOAD DATA INFILE '/data/orders.csv' INTO TABLE orders
      FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
      LINES TERMINATED BY '\n' IGNORE 1 LINES"

# MongoDB：宿主机 Python venv 或 mongoimport
python3 scripts/data-gen/05_load_mongo.py --file data/generated/orders.jsonl

# Kafka：Python producer，按 10 万条一批发送
python3 scripts/data-gen/06_produce_kafka.py \
  --topic orders-json --file data/generated/orders.jsonl --rate 50000

# Elasticsearch：Python bulk，按 5,000 条一批
python3 scripts/data-gen/07_load_es.py \
  --index orders --file data/generated/orders.jsonl
```

### 4.5 Volume Verification

After each system finishes loading, the following must be executed:

```sql
-- PostgreSQL
SELECT pg_size_pretty(pg_total_relation_size('orders'));
SELECT count(*) FROM orders;
```

```sql
-- MySQL
SELECT table_rows,
       ROUND((data_length + index_length) / 1024 / 1024 / 1024, 2) AS size_gib
FROM information_schema.tables
WHERE table_schema = 'source_db' AND table_name = 'orders';
```

```javascript
// MongoDB
db.orders.stats().size        // 逻辑大小
db.orders.stats().storageSize // 物理大小
db.orders.countDocuments()
```

```shell
# Kafka：统计 topic 日志目录大小
docker exec pudding-kafka du -sb /var/lib/kafka/data/orders-json-*
```

```shell
# Elasticsearch
curl -s "localhost:19200/orders/_stats?filter_path=indices.*.primaries.store.size_in_bytes"
```

```shell
# 文件与湖仓
ls -l data/generated/orders.csv
du -sb /tmp/pudding-e2e/warehouse/orders
```

The above commands are executed separately for the three tables `orders`, `customers`, and `payments`, and are finally summarized into one report.
`09_verify_sizes.sh` writes the results to:

```json
{
  "tables": {
    "orders": {
      "rows": 20000000,
      "logicalBytes": 2800000000,
      "systems": {
        "postgres": {"logicalBytes": 4500000000, "rows": 20000000},
        "mysql": {"logicalBytes": 4400000000, "rows": 20000000},
        "mongodb": {"logicalBytes": 4200000000, "rows": 20000000},
        "kafka": {"logicalBytes": 2800000000},
        "elasticsearch": {"logicalBytes": 6000000000},
        "hudi": {"logicalBytes": 2400000000},
        "paimon": {"logicalBytes": 2400000000},
        "iceberg": {"logicalBytes": 2400000000}
      }
    },
    "customers": {"rows": 20000000, "logicalBytes": 2200000000, "systems": {}},
    "payments": {"rows": 20000000, "logicalBytes": 2600000000, "systems": {}}
  }
}
```

If the logical data volume of any of the three primary tables in any system is less than 1 GiB, the script returns non-zero, and the test cannot enter the scale gate.

### 4.6 Dual-Tier Data

| Tier | Rows | Purpose | Location |
| --- | --- | --- | --- |
| `smoke` | 100,000 per table | Daily development, quick reproduction, every regression round | `data/seed/*-smoke.csv` |
| `scale` | 10,000,000 per table | Final acceptance, throughput/memory/spill, incremental scan range | `data/generated/*.csv` |

Rules:

1. All component correctness cases are first run successfully on `smoke`;
2. For `scale`, run only representative cases for each connector/execution mode; do not rerun all 34 components;
3. After finding a bug, first construct a minimal reproduction on `smoke`, then confirm whether `scale` triggers it as well;
4. Record scale results separately; do not mix them with small-data results.

### 4.7 Disk, Memory, and Time Budget

Recommended Docker Desktop configuration:

- Memory: **8–12 GiB** (minimum 8 GiB when loading three 1 GiB tables simultaneously);
- Disk: **≥ 100 GB**;
- CPU: 8 cores or more.

Rough disk usage of the three 1 GiB primary tables (when all systems are retained simultaneously):

| Item | Estimate |
| --- | --- |
| Raw CSV/JSONL | 11–13 GB |
| PostgreSQL/MySQL/MongoDB | 14–22 GB |
| Kafka (three topics) | 4–5 GB |
| Elasticsearch | 6–10.5 GB |
| Hudi/Paimon/Iceberg (one set of three tables each) | 10–11 GB |
| MinIO replicas | 3–4 GB |
| Docker images | ~10 GB |
| **Total** | **approx. 58–75 GB** |

Therefore it is recommended to:

- Keep at least **100 GB** of available disk on the host;
- Set the Docker Desktop disk limit to **≥ 100 GB**;
- Do not retain copies of all systems simultaneously for long periods; load sequentially by profile and delete a system's data volume after testing it;
- Keep only one copy of raw data; each system imports it through loading scripts;
- Run the lakehouse scale gate in the order Hudi → Paimon → Iceberg, cleaning temporary copies after each format completes.

Time budget (one-off):

| Step | Estimate |
| --- | --- |
| Generate three 1 GiB raw datasets | 30–60 minutes |
| PostgreSQL/MySQL import + index (three tables) | 20–40 minutes per system |
| MongoDB import (three tables) | 30–60 minutes |
| Kafka production | 10–30 minutes |
| Elasticsearch import (three tables) | 30–60 minutes |
| Three lakehouse baseline writes (three tables per format) | 30–60 minutes per format |
| Size verification | 20 minutes |

If Docker memory is insufficient, prioritize loading only the systems needed by the current profile; keep the data volumes for reuse by subsequent profiles.

---

## 5. Coverage Matrix

### 5.1 Source (13 types)

| ID | type | Test path | Key assertions |
| --- | --- | --- | --- |
| SR-01 | `file-common-source` | CSV/JSON/Parquet/text → `file-sink` | Row count, schema, options, partitioning |
| SR-02 | `jdbc-source` | PostgreSQL/MySQL table → `file-sink`; incremental append/upsert | Cursor advances, second run incremental is 0 |
| SR-03 | `mysql-source` | MySQL table → `file-sink` | Results consistent with jdbc-source |
| SR-04 | `mongodb-source` | Mongo collection → `file-sink` | Document count, field mapping, options |
| SR-05 | `elasticsearch-source` | ES index → `file-sink` | limit, field types, pagination |
| SR-06 | `kafka-source` | bounded Kafka JSON/raw → `file-sink` | starting/ending offsets, key/value |
| SR-07 | `hive-source` | local Derby Hive table → `file-sink` | Conditional filtering, schema projection |
| SR-08 | `hudi-source` | path/catalog snapshot + incremental → `file-sink` | Internal column filtering, incremental cursor |
| SR-09 | `paimon-source` | path/catalog snapshot + incremental → `file-sink` | `_row_kind_`, scanMode |
| SR-10 | `iceberg-source` | path/catalog snapshot + incremental → `file-sink` | snapshot/branch/tag, incremental |
| SR-11 | `mysql-cdc-source` | MySQL binlog → Paimon/Hudi | insert/update/delete, op column, ordering |
| SR-12 | `postgres-cdc-source` | PostgreSQL logical decoding → Iceberg | Same as above |
| SR-13 | `mongodb-cdc-source` | Mongo change stream → Paimon/Iceberg | insert/update/delete, resume token |

### 5.2 Sink (11 types)

| ID | type | Test path | Key assertions |
| --- | --- | --- | --- |
| SK-01 | `console-sink` | CSV → console | stdout row count, truncate/vertical |
| SK-02 | `file-sink` | CSV → parquet/json/csv | append/overwrite, partitioning, read-back consistency |
| SK-03 | `jdbc-sink` | CSV → PostgreSQL/MySQL | append/overwrite/upsert/cdc, transactions |
| SK-04 | `mysql-sink` | CSV → MySQL | MySQL dialect, reserved words, `ON DUPLICATE KEY` |
| SK-05 | `mongodb-sink` | CSV → Mongo | append/overwrite/update/upsert |
| SK-06 | `elasticsearch-sink` | CSV → ES | index creation, idField, upsert |
| SK-07 | `kafka-sink` | CSV → Kafka | topic/key/value, consumer verification |
| SK-08 | `hive-sink` | CSV → Hive | saveAsTable, partitionBy, overwrite |
| SK-09 | `hudi-sink` | CSV → Hudi path/catalog | append/upsert/cdc/full refresh, idempotency |
| SK-10 | `paimon-sink` | CSV → Paimon path/catalog | Same as above, `_row_kind_` |
| SK-11 | `iceberg-sink` | CSV → Iceberg path/catalog | Same as above, MERGE, snapshot ordering |

### 5.3 Transform (10 types)

| ID | type | Test content |
| --- | --- | --- |
| TR-01 | `filter-transform` | Conditional filtering, `numPartitions` |
| TR-02 | `aggregate-transform` | Grouped aggregation; streaming window + watermark |
| TR-03 | `union-transform` | byName/byPosition, allowMissingColumns, distinct |
| TR-04 | `column-transform` | Select/rename/cast, unknown field error |
| TR-05 | `limit-transform` | limit=0/normal value/negative value error |
| TR-06 | `join-transform` | inner/left/right/full/semi/anti; streaming inner join |
| TR-07 | `validate-transform` | drop/error/quarantine; streaming drop only |
| TR-08 | `dedup-transform` | batch keys; streaming watermark |
| TR-09 | `schema-contract-transform` | Normal/missing column/type mismatch/extra column |
| TR-10 | `sql-transform` | tempView/tempViews, multiple dependencies, unknown view error |

---

## 6. Case Design

### 6.1 L0: Build and Static Pre-checks

| ID | Content | Pass criteria |
| --- | --- | --- |
| L0-01 | `mvn -B clean package -DskipTests` | Build succeeds, distribution package complete |
| L0-02 | `bin/pudding capabilities verify "$PUDDING_HOME"` | Returns 0, jar and manifest consistent |
| L0-03 | Static scan of `build/examples/*` | Each example has `job.mode` and exactly one sink |
| L0-04 | Example required-field check | `job.sparkHome` / `job.sparkPuddingHome` present or overridden by the CLI |
| L0-05 | Placeholder check | `${SPARK_HOME}`, `${PUDDING_HOME}` substituted |
| L0-06 | ServiceLoader / manifest / README comparison | 34 component identifiers consistent |

Known suspicions (verify first):

1. `incremental_jdbc_to_jdbc.yaml`, `read_kafka_avro_example.yaml` lack `job.sparkHome` / `job.sparkPuddingHome`, while the client requires these two values before submission;
2. The client error message says "set PUDDING_HOME", but the code does not read the `PUDDING_HOME` environment variable;
3. `build/examples/data/demo.csv` has 4 columns in the header and 5 columns in data rows;
4. README claims built-in PostgreSQL/MySQL JDBC drivers, but in `libs/` only `cdc-runtime` carries drivers; ordinary `jdbc-source`/`jdbc-sink` may not be able to get them;
5. The Schema Registry image requires amd64 emulation under arm64.

### 6.2 L1: Source Single Component

Execution method:

- Non-CDC: source → `file-sink` (JSON), verified with `jq`/Python;
- CDC: source → Paimon/Iceberg, verified with the streaming flow in Section 6.5;
- Each case runs on `smoke` data first, and then on `scale` for the scale gate.

Assertions:

- Row/document count;
- Sampled values of key fields and checksums;
- Schema field names, types, nullability;
- For incremental cases, the second run adds 0 rows and the cursor is unchanged;
- Logs do not contain connection string passwords or secret values.

### 6.3 L2: Sink Single Component

Execution method: fixed `smoke` CSV → each sink → read back.

Read-back methods:

- JDBC/MySQL: query directly with `psql`/`mysql`;
- Mongo: `mongosh` query;
- ES: `curl _search`;
- Kafka: `kafka-console-consumer` or `kafka-console-consumer.sh`;
- Hive: `spark-sql` or `hive-source → file-sink`;
- Hudi/Paimon/Iceberg: corresponding source → file-sink, plus additional verification through table path file sizes/metadata;
- File/Console: read files/stdout directly.

Key assertions:

- append does not overwrite old data;
- overwrite/full refresh retains only the new snapshot;
- After upsert, each key has exactly one row and it is the latest value;
- CDC delete actually deletes the target row;
- Repeated execution of the same batch is idempotent;
- After a failure, the target table has no half-committed data.

### 6.4 L3: Transform Single Component

| ID | Input | Expectation |
| --- | --- | --- |
| TR-01 | Orders CSV, `amount >= 100` | Row count matches the SQL result |
| TR-02 | Aggregate count/sum by `city` | Grouped results consistent; streaming output by watermark |
| TR-03 | Two isomorphic CSVs | byName/byPosition row count and column order correct |
| TR-04 | Rename + cast | Column names/types correct; unknown field errors |
| TR-05 | limit 0/10 | Row count correct; negative value errors |
| TR-06 | orders + customers | 6 join types; streaming inner only |
| TR-07 | Data containing invalid amounts/empty keys | drop/error/quarantine behavior correct |
| TR-08 | Duplicate `order_id` | One row per key; streaming limited by watermark |
| TR-09 | Normal/missing column/extra column/type change | Pass or fail per contract |
| TR-10 | Single/multiple-dependency SQL | View resolution and results correct; unknown view errors |

Negative cases must assert:

- The failure occurs before Spark starts or before the first action (where early validation is possible);
- The error message contains the configuration key name and does not contain sensitive values;
- The exit code is non-zero.

### 6.5 L4: Modes and End-to-End Golden Paths

| ID | Flow | Mode | Assertion |
| --- | --- | --- | --- |
| E2E-01 | CSV → filter → column → aggregate → limit → file | batch | Composition order, results consistent with the SQL baseline |
| E2E-02 | CSV + CSV → join → union → dedup → file | batch | Multi-dependency DAG, join key merging |
| E2E-03 | CSV → validate(quarantine) + schema-contract → file | batch | Bad rows enter quarantine, good rows continue |
| E2E-04 | PostgreSQL → JDBC incremental upsert → PostgreSQL | sync incremental | Insert/update/delete, replay idempotency |
| E2E-05 | MySQL → MySQL CDC → Paimon | streaming | insert/update/delete, op column |
| E2E-06 | MySQL CDC → Hudi; MySQL CDC → Iceberg | streaming | The same data behaves consistently in three lakehouses |
| E2E-07 | MongoDB CDC → Paimon/Iceberg | streaming | resume token, deletion |
| E2E-08 | PostgreSQL CDC → JDBC sink | streaming | Single-transaction delete + upsert |
| E2E-09 | Kafka JSON → filter/column → Hudi update | streaming | checkpoint, restart without duplication or loss |
| E2E-10 | Kafka Avro/Protobuf → Paimon/Iceberg | streaming | Schema Registry decoding, subject strategy |
| E2E-11 | Kafka → window aggregate / dedup / stream-stream join → Paimon | streaming | watermark, late data, state cleanup |
| E2E-12 | Hudi → Paimon; Iceberg → Hudi | sync incremental | Incremental sync between lakehouses |
| E2E-13 | Hive ↔ file; ES ↔ Mongo; Kafka sink → Kafka source | batch | Bidirectional links, field mapping |
| E2E-14 | file → file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg `sync full` | sync full | Full replacement, old data removal; Paimon/File/Hive require explicit `allowNonAtomicReplace` |
| E2E-15 | S3A checkpoint/state + Iceberg S3FileIO + checkpoint CLI | streaming/sync | MinIO paths, permissions, cleanup |

The scale gate covers at least:

- E2E-01 (large files and transform);
- E2E-04 (1 GiB full + incremental cursor);
- E2E-05 or E2E-06 (CDC initial snapshot 1 GiB);
- E2E-09 (Kafka 1 GiB streaming);
- E2E-14 (lakehouse 1 GiB full replacement).

### 6.6 L5: Fault, Recovery, and Negative Cases

| ID | Scenario | Assertion |
| --- | --- | --- |
| RG-01 | `kill -9` the driver during incremental execution, then rerun | No duplicates, no data loss |
| RG-02 | Two processes run against the same source id simultaneously | Only one acquires the lock, the other clearly fails |
| RG-03 | Sink unavailable after source finishes reading | A pending transaction is left; `state resolve --abort/--commit` can recover |
| RG-04 | Kafka broker restart | checkpoint resumes, offsets not lost |
| RG-05 | MySQL/PostgreSQL restart | CDC recovers from binlog/LSN |
| RG-06 | `delete-on-success` + external `query.stop()` | checkpoint retained, not mistakenly deleted |
| RG-07 | `state reset/unlock/migrate` | Cursor, lock, and JDBC state table behavior correct |
| NG-01 | Multiple sinks | Rejected before startup |
| NG-02 | transform missing dependency/cycle | Clear error |
| NG-03 | source/sink does not support the target mode | Rejected before startup |
| NG-04 | Invalid outputMode | Clear error |
| NG-05 | Missing required config | Error message contains only the key, not the value |
| NG-06 | Duplicate key / invalid op column | Sink rejects |
| NG-07 | Schema drift | Fail per contract or handle per policy |
| NG-08 | `${secret:...}` missing/incorrect | Logs and exceptions do not leak credentials |

Fault injection methods:

- `docker stop/start`: restart databases, Kafka, ES;
- `docker pause/unpause`: pause network/processes;
- `kill -9`: directly kill the driver JVM;
- Port occupation/wrong password: connection failure path;
- Modify checkpoint directory permissions: IO failure path.

### 6.7 L6: Object Storage, HDFS, and Maintenance

| ID | Content | Assertion |
| --- | --- | --- |
| OS-01 | S3A checkpoint + filesystem state (MinIO) | Write/read/cleanup correct |
| OS-02 | Iceberg S3FileIO warehouse | Separate from S3A configuration, credentials not mixed |
| OS-03 | HDFS checkpoint/state | NameNode/DataNode read/write, recovery |
| OS-04 | `checkpoint show/status/delete/cleanup` | JSON fields, dry-run, age calculation |
| OS-05 | `state show/status/resolve/unlock/migrate` | State consistent, lock released |
| OS-06 | `maintenance iceberg/paimon/hudi` | status/compact/expire/clean, `--confirm`/`--dry-run` |
| OS-07 | `maintenance-server` | token, idempotency, cancellation, audit file |
| OS-08 | `capabilities verify/show/diff` | Version differences, missing component detection |

---

## 7. Execution Flow

### 7.1 Phase Breakdown

| Phase | Content | Estimate |
| --- | --- | --- |
| Phase 0 | Environment preparation, compose, build distribution package, L0 static checks | 0.5 day |
| Phase 1 | `smoke` data: 13 source + 11 sink + 10 transform | 2–3 days |
| Phase 2 | `smoke` data: E2E golden paths, four modes | 2–3 days |
| Phase 3 | Generate/load three 1 GiB tables, scale gate cases | 1–2 days loading + 2–3 days execution |
| Phase 4 | Fault recovery, negative cases, object storage/maintenance | 2 days |
| Phase 5 | Regression, defect re-verification, result summary | 1–2 days |

Full execution is estimated at 8–12 working days; if loading sequentially by profile and running only P0, about 5–7 working days.

P0 priority (run first):

1. All of L0;
2. SR-01/02/06/11, SK-02/03/07/09/10/11, TR-01/02/06/07/09;
3. E2E-01/04/05/09/14;
4. RG-01/02/03, NG-01–NG-08;
5. E2E-04/E2E-09/E2E-14 in the 1 GiB scale gate.

Automated P0 entry point:

```bash
scripts/run/bench-suite.sh all
```

Individual phases are available as `preflight`, `smoke`, `prepare`, `scale` and
`report`. The full report is written to `results/bench/suite-<run-id>/report.md`
and copied to `results/bench/benchmark-report.md`.

### 7.2 Single-Case Execution Flow

```text
1. scripts/services/ensure-profile.sh <profile>
2. scripts/data-gen/ensure-smoke.sh / ensure-scale.sh
3. scripts/run/run-case.sh <case-id>
   ├── 渲染 config/jobs/<case-id>.yaml
   ├── 准备目标表/目录/topic/index
   ├── 记录开始时间与源端基线
   ├── bin/pudding submit env job log
   ├── 检查退出码
   ├── 执行 scripts/verify/assert-<case>.sh
   └── 写 results/runs/<case-id>/result.json
4. 失败时保留 job、日志、目标端状态和最小复现命令
```

### 7.3 Data Lifecycle

- `smoke` data: can be rebuilt before each run, seconds to minutes;
- `scale` data: generated once, retained in `data/generated/` and Docker volumes;
- Each case resets only the target tables/directories it uses, without cleaning other people's data;
- Use `scripts/services/reset-all.sh` when a complete reset is needed, then reload;
- After testing, retain the checksums of `results/` and `data/generated/`; deletion of large files is a manual decision.

---

## 8. Assertions and Evidence

### 8.1 Result Directory

```text
results/runs/<case-id>/
  job.yaml
  env.properties
  submit.log
  spark.log
  assert.log
  result.json
```

`result.json` contains at least:

```json
{
  "caseId": "E2E-04",
  "dataset": "scale",
  "status": "passed",
  "startedAt": "2026-11-06T10:00:00Z",
  "finishedAt": "2026-11-06T10:18:00Z",
  "rows": {"source": 20000000, "target": 20000000},
  "checks": [
    {"name": "row_count", "expected": 20000000, "actual": 20000000},
    {"name": "checksum", "expected": "...", "actual": "..."}
  ],
  "artifacts": ["submit.log", "assert.log"]
}
```

### 8.2 Assertion Dimensions

1. **Exit code**: foreground `pudding submit` returns 0 only for `FINISHED`;
2. **Data**: row counts, fields, checksums, sampled comparison;
3. **Idempotency**: executing the same batch twice yields consistent results;
4. **Incremental scope**: the second run scans only the increment, not the full table again;
5. **Recovery**: the final result after kill/restart matches an uninterrupted run;
6. **Resources**: connections, temporary files, checkpoints, and state locks are released correctly;
7. **Security**: logs/exceptions contain no passwords, secret values, or full connection strings;
8. **Scale**: all entries in `results/data-scale/size-report.json` are ≥ 1 GiB.

### 8.3 Independent Verification

Avoid using only spark-pudding to verify itself:

- Use `psql`/`mysql` for relational databases;
- Use `mongosh` for MongoDB;
- Use `_search`/`_count` for ES;
- Use console consumer for Kafka;
- Use `wc`/`sha256sum`/Python for files;
- For lakehouses, preferably read with native Spark SQL; keep at least one `source → file` cross-check.

---

## 9. Defect Management

### 9.1 Severity Levels

| Level | Definition | Example |
| --- | --- | --- |
| P0 | Data loss, duplication, incorrect results, silent success, credential leakage | After upsert, old values overwrite new values; failure but exit code 0 |
| P1 | Feature unavailable, recovery failure, explicit error but cannot continue | CDC cannot start; checkpoint cannot recover |
| P2 | Boundary behavior, error messages, documentation/example issues | Example missing required fields; misleading prompt |
| P3 | UX, performance, non-blocking warnings | Excessive logs, missing UI metrics |

### 9.2 Report Template

Each defect records:

- Title: `[component] brief symptom`
- Environment: branch/commit, JDK, Spark, distribution package path, Docker image tag;
- Data tier: `smoke` / `scale`, row count;
- Configuration: corresponding `config/jobs/<case>.yaml` (sanitized);
- Steps: minimal reproduction command;
- Expected/actual;
- Evidence: `results/runs/<case-id>/` path, key log excerpts;
- Impact: data correctness/recovery/performance/security;
- Initial localization: source/sink/transform/core/runtime.

After a defect is fixed, the following must be done:

1. Regress on `smoke`;
2. Re-verify on the `scale` case that triggered the defect;
3. Update the status in `results/`;
4. Add an automated assertion to prevent regression.

---

## 10. Risks and Pre-checks

### 10.1 Environment Risks

| Risk | Mitigation |
| --- | --- |
| Insufficient Docker memory | Adjust to 8–12 GiB; start/stop by profile; run ES/Kafka separately |
| Insufficient Docker disk | Set ≥100 GB; keep only the data volumes of the current profile |
| Missing arm64 images | Use amd64 emulation or host processes for Schema Registry/HDFS |
| Kafka advertised listener misconfigured | Use `localhost:19092` on the host and `kafka:29092` inside containers |
| Mongo replica set host inconsistency | Use `localhost:27018` during initialization |
| MySQL authentication plugin | Add `allowPublicKeyRetrieval=true&useSSL=false` to the URL |
| Port conflicts | Uniformly use ports 15432/13306/27018/19092/18081/19200, etc. |
| No GNU timeout on macOS | Install coreutils or use `gtimeout`/Python timeout |

### 10.2 Data Risks

- Generating/loading 1 GiB data takes a long time and must be cached, not rebuilt for every case;
- Data generation must use a fixed seed to ensure reproducibility;
- Time fields uniformly use UTC to avoid time zone and daylight saving interference;
- Amounts use `decimal` to avoid floating-point errors;
- Delete/update increments must have explicit id lists so expected results can be computed;
- Large-data assertions must not use `collect()`; use count/sum/checksum/sampling.

### 10.3 Known Pre-checks

Complete the following before starting Docker:

1. Build the distribution package and run `capabilities verify`;
2. Statically check the required fields and placeholders of all examples;
3. Run a minimal `file → file` job with `smoke` data to confirm that the Spark, JDK, and distribution package paths are correct;
4. Run a `jdbc-source` without an extra JDBC driver via `pudding submit` to confirm whether the driver is truly built in;
5. Confirm that Docker Compose's Kafka/Schema Registry can start under arm64;
6. Confirm that the Docker disk limit is ≥ 100 GB before starting 1 GiB data loading.

---

## 11. Follow-up Implementation Checklist

Implement in order:

1. `config/docker/docker-compose.yml` + initialization SQL;
2. `config/env/*.properties` templates;
3. `scripts/services/`: start/stop, health checks, reset;
4. `scripts/data-gen/`: smoke/scale generation, per-system loading, size verification;
5. `config/jobs/`: minimal jobs for 34 components + 15 E2E jobs;
6. `scripts/run/run-case.sh`: unified executor;
7. `scripts/verify/`: row count, checksum, state, checkpoint, and log assertions;
8. `scripts/services/reset-data.sh`;
9. `docs/bug-report-template.md` and result summary template;
10. Run P0 first, then decide whether to extend to the full 1 GiB gate.

### 11.1 Current Implementation Status (2026-11-06)

- ✅ `config/docker/docker-compose.yml`: relational/streaming/search/infra/hdfs
  profiles, initialization SQL, health checks;
- ✅ `scripts/services/up.sh`, `down.sh`, `status.sh`, `init-mongo.sh`,
  `reset-data.sh`;
- ✅ `scripts/data-gen/01_generate_tables.py`, `02_generate_delta.py`,
  CSV/JSONL format and row counts have been verified with smoke data;
- ✅ `03_load_postgres.sh`, `04_load_mysql.sh`, `05_load_mongo.py`,
  loading has been completed with smoke data;
- ✅ `06_produce_kafka.py` JSON, Avro, and Protobuf, verified with local Kafka +
  Schema Registry;
- ✅ `07_load_es.py`, verified with local Elasticsearch;
- ✅ `08_build_lakehouse.sh` / `08_build_lakehouse.py`, Hudi/Paimon/Iceberg
  each completed baseline writes with 1,000 rows;
- ✅ `09_verify_sizes.py` / `09_verify_sizes.sh`, the 1 GiB threshold failure path has been verified;
- ✅ Complete loading and gate for 1 GiB scale data:
  `results/data-scale/size-report.json` has `passed=true`, with no failure/missing;
- ✅ 34 component job templates (`config/jobs/`);
- ✅ `scripts/run/` case executor and basic output assertions;
- ✅ P0 smoke: 30 finite batch cases, 30 PASS, 0 FAIL;
- ✅ Elasticsearch connector P0 defect fixed (Scala 2.12 runtime):
  `results/runs/ES-connector-bug.md`;
- ✅ Local client silent success P0 defect fixed:
  `results/runs/client-exit-code-bug.md`;
- ✅ Sink target-side assertions: `scripts/verify/verify-case.sh`; Spark
  table read row-count assertions were added for SK-09/10/11 (`scripts/verify/table-check.sh` + `paimon-check.py`);
- ✅ E2E-01 combined transform chain PASS;
- ✅ E2E-02 union → inner join → projection → dedup PASS (15,384 rows);
- ✅ E2E-03 validate(drop+quarantine) + schema-contract PASS
  (7 valid rows, 3 rows quarantined);
- ✅ E2E-08 PostgreSQL CDC → JDBC sink: snapshot, delete + upsert in a single transaction,
  checkpoint restart PASS;
- ✅ E2E-07 MongoDB CDC → Paimon: snapshot, delete by `_id`, upsert,
  resume token, checkpoint restart PASS;
- ✅ E2E-06 MySQL CDC → Hudi/Iceberg: snapshot, upsert, delete,
  checkpoint restart PASS;
- ✅ CDC smoke: SR-11 (MySQL), SR-12 (PostgreSQL, 10,000-row initial snapshot),
  SR-13 (MongoDB, 10,000-row snapshot + I/U/D) PASS;
  `results/runs/cdc-smoke-summary.md`;
- ✅ E2E-04 PostgreSQL → JDBC incremental upsert → PostgreSQL PASS
  (initial full load, 2 inserts, 1 update, no-change resumption, replay idempotency);
- ✅ E2E-05 MySQL CDC → Paimon PASS
  (snapshot, upsert, delete, checkpoint restart, continued incremental after restart);
- ✅ E2E-09 Kafka JSON → filter/column → Hudi PASS
  (10,000 records consumed, 3 incremental records consumed after checkpoint restart, upsert updates, no duplicates);
- ✅ `scripts/run/run-e2e.sh`: multi-step E2E orchestration and process-group timeout cleanup;
- ✅ `scripts/run/run-object-store.sh`: MinIO fixture, S3A checkpoint/state,
  Iceberg S3FileIO, and object storage orchestration for the checkpoint CLI;
- ✅ `scripts/run/run-hdfs.sh`, `run-state-backends.sh`, `run-maintenance.sh`,
  `run-maintenance-server.sh`, `run-capabilities.sh`: orchestration for
  OS-03/05/06/07/08;
- ✅ `scripts/run/run-join-e2e.sh`, `run-schema-registry-subject.sh`:
  E2E-11 self-join and Schema Registry subject/error path orchestration;
- ✅ E2E-14 `sync full` seven sinks: file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg
  each ran 100 rows, then were replaced with 200 rows, all PASS;
- ✅ RG-01 `kill -9` the driver during incremental write: a pending transaction is left,
  rerun after `state unlock`, target 10,000 rows, no duplicates, pending cleared;
- ✅ RG-02 concurrent incremental job: the second job is rejected by the state lock, the first job writes
  200,000 rows and then commits normally;
- ✅ RG-03 sink unavailable leaves a pending transaction: after `resolve --abort`,
  replay writes 10,000 rows; `resolve --commit` skips unwritten batches, target remains 0;
- ✅ RG-04 Kafka broker restart: after consuming the first 10,000 records, restart the broker, then write
  2 more; checkpoint resumes, output 10,002 records with no duplicates;
- ✅ RG-05 CDC resumes after MySQL restart: both rows before/after the restart are captured; the Debezium binlog
  client reconnects by itself after about 2 minutes, see `results/runs/recovery-summary.md`;
- ✅ RG-07 `state status/show/reset/unlock/migrate/list`: JSON fields,
  cursor reset and recovery, no-lock prompt, filesystem target rejects migrate;
- ✅ Negative cases NG-01/02/03/04/05/07/08: multiple sinks, unknown dependency, streaming mode unsupported,
  invalid outputMode, missing `url`, missing schema column, missing secret all fail before startup,
  and error messages contain only key names and do not leak credentials
  (`results/runs/negative-summary.md`);
- ✅ Scale gate E2E-01: filter/column/aggregate/
  limit completed on 1.3 GiB `orders.csv`, output 10 rows;
- ✅ Scale gate E2E-04: 10,000,000 rows PostgreSQL → JDBC initial full load, then insert
  2 rows and update 1 row; target 10,000,002 rows. The scale template uses an 8g driver heap,
  32 shuffle partitions, and disables AQE; `JdbcIncremental` has been changed to
  repartition by business key, avoiding single-task write OOM;
- ✅ E2E-10 Kafka Avro/Protobuf + Schema Registry → Paimon:
  10,000 records of each encoding, decoded via the Confluent wire format and written to Paimon;
- ✅ E2E-11 stateful streaming: 5-minute window aggregation (1-minute watermark) closed 4 windows,
  600 events in total; dedup outputs 1,000 rows for 1,000 unique keys + 100 duplicates;
- ✅ E2E-12a lakehouse incremental sync: 1,000 rows Hudi → Paimon initial full load, Hudi
  upsert 2 inserts + 1 update, then the second sync reads only the new commit, Paimon target
  1,002 rows;
- ✅ E2E-12b Iceberg → Hudi: 1,000-row full load then append 2 rows, the second sync
  reads the new snapshot, Hudi target 1,002 rows;
- ✅ E2E-13 cross-system round trip: Hive, Kafka, MongoDB, Elasticsearch
  each write 100,000 rows and read back 100,000 rows; the ES stage is executed separately after stopping Kafka/Mongo
  (Docker Desktop VM 3.8 GiB, ES with 1g heap is still OOM-killed);
- ✅ E2E-15 / OS-01 / OS-02 / OS-04 object storage:
  S3A checkpoint remains non-duplicated across restart and new files (10 → 10 → 12 rows);
  S3A filesystem state is rejected when `allowBestEffortLock` is not set, and after enabling it,
  100 → 102 rows with no duplicates; `state status` shows the committed cursor, no pending, and no lock;
  Iceberg JDBC catalog + S3FileIO writes and reads back 1,000 rows;
  a restricted MinIO user verifies that S3A credentials cannot read the warehouse and S3FileIO credentials cannot read the
  checkpoint, and writes with wrong credentials return 403; the checkpoint CLI's JSON fields,
  dry-run, `--keep-last`, `--older-than`, invalid parameters, and security rejections all pass
  (`results/runs/object-store-summary.md`);
- ✅ RG-06 checkpoint retention semantics: `delete-on-success` + `available-now`
  naturally completes and deletes the checkpoint (10 rows); when externally stopped by a 3-second `timeoutMs`,
  the checkpoint is retained and records "did not prove that its input was drained";
- ✅ NG-02 dependency cycle: when two transforms depend on each other, the following is reported before startup:
  `transform dependencies contain a cycle`;
- ✅ NG-06 streaming CDC validation: invalid op value, missing op column, and missing ordering field
  all fail during query execution, and the error message contains the column name and invalid value;
- ✅ E2E-14 Iceberg 1.3 GiB `sync full` scale case: the existing 200-row target is
  atomically replaced with 10,000,000 rows (commit metrics `removedRecords=200`,
  `addedRecords=10000000`);
- ✅ OS-03 HDFS: checkpoint 10 → 10 → 12 rows; after NameNode/DataNode restart,
  both checkpoint and incremental state recover (50 → 51 rows, no duplicates);
- ✅ OS-05 state backends: JDBC/Redis/etcd each 50 → 51 rows, fencing token
  increments, lock released; JDBC `state migrate` is idempotent, reset/restore correct;
- ✅ OS-06 table maintenance: Iceberg/Paimon/Hudi status, compact/rewrite,
  expire, orphan, archive, plus `--confirm`/`--dry-run` and
  CopyOnWrite/path-only rejection;
- ✅ OS-07 maintenance-server: token authentication, idempotency, queued cancellation, rejection of cancellation for completed tasks,
  confirmation gate, audit does not contain token;
- ✅ OS-08 capabilities: verify/show/diff, missing jar and missing manifest;
- ✅ Hudi streaming append empty microbatch: fixed
  `Positive number of partitions required`, covered by the 10M-row CDC snapshot;
- ✅ CDC queue capacity self-deadlock: when `queueCapacity == maxRecordsPerBatch`, the queue
  fills up and the previous batch never commits and the poller blocks; the actual capacity is now at least
  `maxRecordsPerBatch + 1`;
- ✅ E2E-06 MySQL CDC 10,000,013-row initial snapshot → Hudi: the first run wrote
  6,200,031 rows and then failed because the local Hudi HTTP marker server address was unreachable; resuming from the
  checkpoint with `hoodie.write.markers.type=DIRECT` + `upsert`, the final Hudi table is exactly
  10,000,013 rows, and snapshot replay is idempotent;
- ✅ E2E-09 Kafka 10,000,000-record JSON streaming scale case: 4-partition
  `orders-scale` topic, production at about 301k msg/s; Hudi bulk insert finally
  10,000,000 rows, 484 MB (`results/runs/kafka-scale-summary.md`);
- ✅ E2E-11 self-join: derive PAID left and right streams from the same Kafka source, perform an
  inner stream-stream join by `order_id`, with a 1-minute watermark on both sides; first run 5 rows, 0 duplicates
  after checkpoint restart (`results/runs/join-e2e-summary.md`);
- ✅ Schema Registry subject/error path: custom subject decodes 1,000 rows;
  the wire format's schema id takes precedence, and a missing subject does not affect id-based messages;
  unknown schema id 999999 fails and the error contains the topic/id
  (`results/runs/schema-registry-subject-summary.md`);
- ✅ Complete `mvn clean verify` (JDK 11): all 61 modules SUCCESS,
  0 failures (2026-11-08; 7h44m total including host sleep);
- ✅ All scale gates in the test plan completed: E2E-01/04/06/09/14.

Configuration requirements confirmed during implementation:

- Spark/Hudi must use **arm64 JDK 11 or 17**; the default shell's JDK 8 causes
  `spark-class` to fail to find Java. The lakehouse scripts support the `PUDDING_JAVA_HOME` override.
- The MySQL container must not use the read-only `/data` as `--secure-file-priv`, otherwise the entrypoint fails during
  `chown`; currently `--secure-file-priv=` is used to disable the directory check.
- Schema Registry's `kafkastore.bootstrap.servers` must use
  `PLAINTEXT://kafka:29092`, not the custom listener name `INTERNAL://` directly.
- MongoDB uses container port 27018 to ensure that the replica set member `localhost:27018` resolves both inside and outside the container;
  the replica set must be initialized by `init-mongo.sh` after the container starts.
- Hudi requires `spark.serializer=KryoSerializer`; non-partitioned tables require
  `NonpartitionedKeyGenerator` and an empty `partitionpath.field`.
- Paimon requires `spark.sql.extensions=PaimonSparkSessionExtensions` and
  `spark.sql.catalog.paimon`; baseline tables use a primary key + bucket=4.
- Paimon 1.3.2's `file.compression=none` triggers the
  `CompressionCodecName.NONE` exception (the Parquet enum only has `UNCOMPRESSED`), so
  the Paimon baseline uses default compression; the scale gate measures by the logical size of the uncompressed source CSV, and the filesystem
  physical size is recorded separately.
- The Iceberg baseline uses a Hadoop catalog; `write.parquet.compression-codec` must use
  `uncompressed`, and `none` is rejected.
- The local `HADOOP_CONF_DIR`'s `fs.defaultFS=hdfs://localhost:9000` conflicts with MinIO's
  9000 port; local state/checkpoint paths without a scheme are resolved by Hadoop to the
  default HDFS, reporting `RPC response exceeds maximum data length`. Test scripts `unset HADOOP_CONF_DIR` by default,
  and only HDFS cases set `PUDDING_USE_HADOOP_CONF=1`.
- The HDFS container's named volume is created by root and the process runs as `hadoop`; before the first start,
  run `chown -R hadoop:hadoop` on `/tmp/hadoop-root` and run
  `hdfs namenode -format`; the host client must set
  `dfs.client.use.datanode.hostname=true`, otherwise it gets the container IP
  (`172.21.x.x:19866`). After a NameNode restart, wait for `dfsadmin -safemode wait`
  to exit safe mode before submitting jobs.
- Docker Desktop's default VM memory is about 3.8 GiB; when Kafka, Schema Registry,
  MongoDB, PostgreSQL, and MySQL run simultaneously, ES is still OOM-killed even with 1g heap
  (exit 137). Run the ES stage of E2E-13 after stopping Kafka/Mongo, or increase Docker
  VM memory.
- Paimon's `supportsAtomicReplace=false` (replacement is not a single staged commit), so
  `sync full` must explicitly set `allowNonAtomicReplace=true`; catalog mode can create tables with
  `saveAsTable`, but does not automatically create the namespace, so
  `CREATE DATABASE` is required first; path mode overwrite does not automatically create tables, so the table must be created first.

Completed scale measurements (unit GiB, logical data volume):

| System | orders | customers | payments | Result |
| --- | --- | --- | --- | --- |
| CSV | 1.321 | 1.450 | 1.197 | Pass |
| PostgreSQL | 1.871 | 2.142 | 1.830 | Pass |
| MySQL | 1.992 | 1.784 | 1.679 | Pass |
| MongoDB | 2.395 | 2.326 | 2.114 | Pass |
| Kafka (orders topic) | 2.791 | — | — | Pass |
| Hudi | 1.321 | 1.450 | 1.197 | Pass (physical 1.43/1.71/1.43) |
| Paimon | 1.321 | 1.450 | 1.197 | Pass (physical 0.37/0.31/0.35) |
| Iceberg | 1.321 | 1.450 | 1.197 | Pass (physical 1.02/1.30/1.01) |
| Elasticsearch | 1.617 | 1.237 | 1.463 | Pass |

Acceptance completion criteria:

- All 34 components have at least one passing record;
- All 15 E2E golden paths have been run at least once;
- All primary test tables in `results/data-scale/size-report.json` are ≥ 1 GiB;
- Negative/recovery cases have explicit pass or known-defect records;
- All failed items have a minimal reproduction and a defect ID.
