# spark-pudding-benchmark — v1.0.0 test release record

English | [简体中文](RELEASE.md)

- Record date: 2026-11-18
- Version under test: `spark-pudding v1.0.0`
- Tag under test: `v1.0.0` → `2042a5e`
- This project: `1.0.0`
- Development version: `1.0.1-SNAPSHOT`
- Environment: single-node macOS + Docker Desktop
- Conclusion: **functional, recovery, object-storage, negative and scale gates all PASS**

## 1. Goals

This round was not just performance data collection. It answered four questions:

1. Can the batch, sync and streaming modes of `v1.0.0` run end to end on real Spark?
2. Do source, transform and sink combinations and dependency checks match the design?
3. Are CDC, incremental state, locks, fencing, checkpoint restart and failure recovery reliable?
4. At 10M-row scale, can lakehouse, JDBC and Kafka paths complete with exact row counts?

## 2. Environment

| Item | Configuration |
| --- | --- |
| Host | macOS, Apple Silicon |
| JDK | Zulu JDK 11 |
| Spark | 3.4.0 |
| Spark master | `local[2]` |
| Driver memory | 8 GiB for 10M cases; per-case for others |
| Docker | Docker Desktop on the same host |
| Docker memory | ~3.8 GiB available; one heavy profile at a time |
| PostgreSQL | 16, port 15432 |
| MySQL | 8.0, port 13306 |
| MongoDB | 6, port 27018 |
| Kafka | 3.x, port 19092 |
| Schema Registry | port 18081 |
| Elasticsearch | 8.x, port 19200 |
| MinIO | `quay.io/minio/minio`, ports 19000/19001 |
| HDFS | NameNode 18020 |

All services come from `config/docker/docker-compose.yml`; the Compose project name is
`pudding-test`.

## 3. Test process

### 3.1 Build the distribution

```bash
cd /path/to/spark-pudding
mvn -B clean verify
```

Result:

```text
61 modules BUILD SUCCESS
0 test failures
Distribution check: all 43 module jars present
Final verification on 2026-11-17 took 12m37s
```

Artifacts:

```text
build/target/spark-pudding-1.0.0/
build/target/spark-pudding-1.0.0.tar.gz
```

### 3.2 Start the test environment

Start one Docker profile as required by the case:

```bash
cd /path/to/spark-pudding-benchmark

scripts/services/up.sh relational
scripts/services/status.sh

scripts/services/down.sh relational
scripts/services/up.sh streaming
```

Profiles:

| Profile | Services | Purpose |
| --- | --- | --- |
| `relational` | PostgreSQL, MySQL, MongoDB | JDBC, CDC, incremental cases |
| `streaming` | Kafka, Schema Registry | Kafka/CDC streaming cases |
| `search` | Elasticsearch | ES source/sink |
| `infra` | MinIO, Redis, etcd | Object storage and state backends |
| `hdfs` | NameNode, DataNode | HDFS checkpoint/state |

### 3.3 Generate and load data

Smoke data for functional cases:

```bash
python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/generated --formats csv,jsonl --force

scripts/data-gen/03_load_postgres.sh
scripts/data-gen/04_load_mysql.sh
scripts/data-gen/setup-python.sh
.venv/bin/python scripts/data-gen/05_load_mongo.py \
  --data-dir data/generated --tables orders,customers,payments
```

Scale data for scale gates:

```bash
python3 scripts/data-gen/01_generate_tables.py \
  --profile scale --out data/generated --formats csv,jsonl --force

scripts/data-gen/09_verify_sizes.sh
```

Scale constraints:

- `orders`, `customers` and `payments` each hold >= 1 GiB of uncompressed logical data;
- The 1 GiB data set is generated once and reused;
- `data/generated/` is not committed.

### 3.4 Run functional cases

P0 smoke:

```bash
scripts/run/run-smoke-suite.sh
```

Result: **30/30 PASS**, including sink-side assertions.

E2E:

```bash
scripts/run/run-case.sh E2E-01 --dataset smoke
scripts/run/run-e2e.sh E2E-01
```

Coverage:

| Case | Verification |
| --- | --- |
| E2E-01 | filter/column/aggregate transform chain |
| E2E-02 | union → join → dedup |
| E2E-03 | validate/quarantine + schema contract |
| E2E-04 | PostgreSQL → JDBC incremental upsert |
| E2E-05 | MySQL CDC → Paimon |
| E2E-06 | MySQL CDC → Hudi / Iceberg |
| E2E-07 | MongoDB CDC → Paimon |
| E2E-08 | PostgreSQL CDC → JDBC |
| E2E-09 | Kafka JSON → filter/column → Hudi |
| E2E-10 | Kafka Avro/Protobuf + Schema Registry → Paimon |
| E2E-11 | watermark, window aggregation, stream-stream join, dedup |
| E2E-12 | Hudi → Paimon and Iceberg → Hudi incremental sync |
| E2E-13 | Hive, Kafka, MongoDB and Elasticsearch round trips |
| E2E-14 | file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg full refresh |
| E2E-15 | object storage and checkpoint/state paths |

### 3.5 Run matrix, recovery and negative cases

```bash
scripts/run/run-recovery.sh
scripts/run/run-negative.sh
scripts/run/run-object-store.sh
scripts/run/run-hdfs.sh
scripts/run/run-state-backends.sh
scripts/run/run-maintenance.sh
scripts/run/run-maintenance-server.sh
scripts/run/run-capabilities.sh
```

Results:

- RG-01 kill -9 driver recovery;
- RG-02 concurrent locks;
- RG-03 pending transaction abort/commit;
- RG-04 Kafka broker restart and resume;
- RG-05 CDC resume after MySQL restart;
- RG-06 checkpoint deletion/timeout retention;
- RG-07 state CLI;
- NG 11/11 PASS, no credential leaks in error messages;
- OS-01/02/03/04/05/06/07/08 PASS;
- SR-11/12/13 CDC smoke PASS.

### 3.6 Run scale gates

```bash
scripts/run/bench-case.sh E2E-04-scale --rows 10000002 \
  --result-case E2E-04-scale -- scripts/run/run-e2e.sh E2E-04-scale

scripts/run/bench-case.sh E2E-06-hudi-scale --rows 10000013 \
  --result-case E2E-06-hudi-mysql-cdc-scale -- scripts/run/run-e2e.sh E2E-06-scale

scripts/run/bench-case.sh E2E-09-scale --rows 10000000 \
  --result-case E2E-09-kafka-to-hudi-scale -- scripts/run/run-kafka-scale.sh
```

Scale gates:

| Case | Data | Assertion |
| --- | --- | --- |
| E2E-01 | 1.3 GiB CSV | transform returns 10 aggregate rows |
| E2E-14 Iceberg | 1.3 GiB CSV | 200-row target atomically replaced by 10,000,000 rows |
| E2E-14 Hudi | 1.3 GiB CSV | new 10,000,000-row Hudi table |
| E2E-14 Paimon | 1.3 GiB CSV | new 10,000,000-row Paimon table |
| E2E-04 | 10M PostgreSQL | full + 2 inserts + 1 update = 10,000,002 rows |
| E2E-06 Hudi | 10,000,013 MySQL CDC rows | exact Hudi row count |
| E2E-06 Iceberg | 10,000,013 MySQL CDC rows | exact Iceberg row count |
| E2E-09 | 10,000,000 Kafka JSON messages | exact Hudi row count, no duplicates after restart |

### 3.7 Collect metrics

`bench-case.sh` performs the following per case:

- records wall time;
- enables the Spark event log;
- samples driver RSS;
- samples Docker container memory/CPU;
- invokes `parse-spark-events.py` for SQL/job durations, task counts, input bytes, spill and shuffle;
- writes `results/bench/<case>-<timestamp>/metrics.json`.

Summarize:

```bash
scripts/run/bench-summary.py results/bench
```

### 3.8 Verify and clean up

```bash
scripts/verify/table-check.sh hudi orders --count 10000013
scripts/verify/verify-case.sh E2E-06
scripts/run/run-capabilities.sh
```

After verification, stop the services:

```bash
scripts/services/down.sh relational
scripts/services/down.sh streaming
scripts/services/down.sh search
scripts/services/down.sh infra
scripts/services/down.sh hdfs
```

## 4. Benchmark results

| Case | Rows | Wall | Effective completion | Peak RSS | Spill | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| E2E-01 1.3 GiB transform | 10 output rows | 23 s | 23 s | 1.05 GiB | 0 | PASS |
| E2E-14 Iceberg 1.3 GiB full refresh | 10,000,000 | 25 s | 25 s | 1.55 GiB | 0 | PASS |
| E2E-14 Hudi 1.3 GiB full refresh | 10,000,000 | 244 s | 244 s | 4.69 GiB | 0 | PASS |
| E2E-14 Paimon 1.3 GiB full refresh | 10,000,000 | 27 s | 27 s | 1.48 GiB | 0 | PASS |
| E2E-04 10M PostgreSQL incremental | 10,000,002 | 163 s | 146.3 s + 6.8 s | 4.77 GiB | 0 | PASS |
| E2E-06 10M MySQL CDC to Hudi | 10,000,013 | 4855 s | 3037 s | 8.68 GiB | 0 | PASS |
| E2E-06 10M MySQL CDC to Iceberg | 10,000,013 | 1021 s | 1021 s | 2.46 GiB | 0 | PASS |
| E2E-09 10M Kafka to Hudi | 10,000,000 | 1001 s | 195 s | 2.52 GiB | 0 | PASS |

Notes:

- E2E-04 wall time covers the full bootstrap and the incremental 2-insert/1-update Spark applications;
- E2E-06 Hudi completion is measured from the last Hudi commit; 3037 s is the effective completion time, not the runner idle timeout;
- E2E-06 Iceberg stopped when `total-records` reached 10,000,013; 1021 s is the data-completion time;
- E2E-09's 195 s starts from the first Hudi commit and excludes later idle waiting;
- All results are single-node local measurements, not production SLAs.

## 5. Defects found and fixed

| Issue | Impact | Resolution |
| --- | --- | --- |
| Local client reported silent success | Job failed but the process returned 0 | Fixed exit code and error propagation |
| ES connector unavailable on Scala 2.12 | ES source/sink failed at runtime class loading | Fixed binary compatibility and dependencies |
| Hudi streaming append with empty microbatch | 0-row commit failed with `Positive number of partitions required` | Empty batches advance the checkpoint without writing Hudi |
| CDC queueCapacity self-deadlock | After the queue filled, Spark stopped submitting batches and the poller blocked | Capacity is now at least `maxRecordsPerBatch + 1` |
| Hudi 10M CDC marker server unreachable | First run failed after writing 6,200,031 rows | Resumed from checkpoint with `hoodie.write.markers.type=DIRECT` and `upsert` |
| MinIO image removed from Docker Hub | Container tests returned 404 | Switched to `quay.io/minio/minio` |

## 6. Known limitations

- Single-node Docker environment with limited resources; not representative of a production cluster;
- E2E-06 Hudi 10M is a long job; the default runner timeout is 80 minutes;
- Hudi/Paimon 10M full refresh creates new tables; Iceberg scale needs a pre-created table;
- Hudi Jackson-encoded DECIMAL is unrecoverable during CDC replay; the product fails closed;
- PostgreSQL inactive replication slots may retain large WAL volumes; clean them before a 10M write;
- Benchmarks cover only the current host and distribution; hardware, JDK and Docker memory affect results;
- GitHub Actions does not run the full benchmark; CI covers builds and container regressions only.

## 7. Reproduction

Run the complete benchmark with one command:

```bash
scripts/run/bench-suite.sh all
```

Or run individual phases:

```bash
scripts/run/bench-suite.sh preflight
scripts/run/bench-suite.sh smoke
scripts/run/bench-suite.sh prepare
scripts/run/bench-suite.sh scale
scripts/run/bench-suite.sh report
```

Manual step-by-step reproduction:

```bash
cd /path/to/spark-pudding
mvn -B clean verify

cd /path/to/spark-pudding-benchmark
scripts/services/up.sh relational

python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/seed --formats csv,jsonl --force
PUDDING_DATA_DIR="$PWD/data/seed" PUDDING_DATA_CONTAINER_DIR=/data-seed \
  scripts/data-gen/03_load_postgres.sh
PUDDING_DATA_DIR="$PWD/data/seed" PUDDING_DATA_CONTAINER_DIR=/data-seed \
  scripts/data-gen/04_load_mysql.sh

scripts/run/run-smoke-suite.sh file-basic smoke
scripts/run/run-smoke-suite.sh relational smoke
scripts/run/run-case.sh E2E-01 --dataset smoke

scripts/services/down.sh relational
```

Full regression:

```bash
scripts/run/run-e2e.sh E2E-01
scripts/run/run-recovery.sh
scripts/run/run-negative.sh
scripts/run/run-object-store.sh
scripts/run/run-hdfs.sh
scripts/run/run-state-backends.sh
scripts/run/run-maintenance.sh
scripts/run/run-maintenance-server.sh
scripts/run/run-capabilities.sh
```

## 8. Conclusion

- Functional: P0 smoke 30/30 PASS; E2E-01 through E2E-15 all PASS;
- Recovery: RG-01 through RG-07 PASS;
- Negative: NG 11/11 PASS, no credential leaks;
- Object storage: OS-01/02/03/04/05/06/07/08 PASS;
- Scale: E2E-01/04/06/09/14 all gates PASS;
- Build: `mvn -B clean verify` succeeded for all 61 modules with 0 failures.

This round satisfies the local regression release criteria for `spark-pudding v1.0.0`.
