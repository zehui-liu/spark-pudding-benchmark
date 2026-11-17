# spark-pudding-benchmark — v1.0.0 测试发布记录

[English](RELEASE.en.md) | 简体中文

- 记录日期：2026-11-18
- 被测版本：`spark-pudding v1.0.0`
- 被测 tag：`v1.0.0` → `2042a5e`
- 本项目版本：`1.0.0`
- 开发版本：`1.0.1-SNAPSHOT`
- 测试环境：单节点 macOS + Docker Desktop
- 结论：**功能、恢复、对象存储、负向和规模门禁全部 PASS**

## 1. 测试目标

本轮测试不是单纯收集性能数字，而是回答四个问题：

1. `v1.0.0` 的 batch、sync、streaming 三种模式能否在真实 Spark 上端到端运行；
2. source、transform、sink 的组合和依赖校验是否符合设计；
3. CDC、增量状态、锁、fencing、checkpoint 重启和故障恢复是否可靠；
4. 10M 行级别的数据量下，湖仓、JDBC 和 Kafka 链路能否完成并保持精确行数。

## 2. 测试环境

| 项目 | 配置 |
| --- | --- |
| 主机 | macOS，Apple Silicon |
| JDK | Zulu JDK 11 |
| Spark | 3.4.0 |
| Spark master | `local[2]` |
| Driver 内存 | 10M 用例 8 GiB，其余按用例配置 |
| Docker | Docker Desktop，同机运行 |
| Docker 内存 | 约 3.8 GiB 可用，一次只跑一个重 profile |
| PostgreSQL | 16，端口 15432 |
| MySQL | 8.0，端口 13306 |
| MongoDB | 6，端口 27018 |
| Kafka | 3.x，端口 19092 |
| Schema Registry | 端口 18081 |
| Elasticsearch | 8.x，端口 19200 |
| MinIO | `quay.io/minio/minio`，端口 19000/19001 |
| HDFS | NameNode 18020 |

所有服务由 `config/docker/docker-compose.yml` 提供，Compose 项目名为 `pudding-test`。

## 3. 测试过程

### 3.1 构建被测发行包

```bash
cd /path/to/spark-pudding
mvn -B clean verify
```

验证结果：

```text
61 modules BUILD SUCCESS
测试失败 0
发行包校验：43 个模块 jar 全部存在
2026-11-17 最终验证耗时 12m37s
```

产物：

```text
build/target/spark-pudding-1.0.0/
build/target/spark-pudding-1.0.0.tar.gz
```

### 3.2 启动测试环境

按用例需要启动一个 Docker profile：

```bash
cd /path/to/spark-pudding-benchmark

scripts/services/up.sh relational
scripts/services/status.sh

scripts/services/down.sh relational
scripts/services/up.sh streaming
```

profile 与用途：

| Profile | 服务 | 用途 |
| --- | --- | --- |
| `relational` | PostgreSQL、MySQL、MongoDB | JDBC、CDC、增量用例 |
| `streaming` | Kafka、Schema Registry | Kafka/CDC 流式用例 |
| `search` | Elasticsearch | ES source/sink |
| `infra` | MinIO、Redis、etcd | 对象存储和 state 后端 |
| `hdfs` | NameNode、DataNode | HDFS checkpoint/state |

### 3.3 生成和加载数据

smoke 数据用于功能用例：

```bash
python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/generated --formats csv,jsonl --force

scripts/data-gen/03_load_postgres.sh
scripts/data-gen/04_load_mysql.sh
scripts/data-gen/setup-python.sh
.venv/bin/python scripts/data-gen/05_load_mongo.py \
  --data-dir data/generated --tables orders,customers,payments
```

scale 数据用于规模门禁：

```bash
python3 scripts/data-gen/01_generate_tables.py \
  --profile scale --out data/generated --formats csv,jsonl --force

scripts/data-gen/09_verify_sizes.sh
```

规模约束：

- `orders`、`customers`、`payments` 三张主表各 ≥ 1 GiB 未压缩逻辑数据；
- 1 GiB 数据只生成一次并复用；
- `data/generated/` 不进入 Git。

### 3.4 执行功能用例

P0 smoke：

```bash
scripts/run/run-smoke-suite.sh
```

结果：**30/30 PASS**，包含 sink 目标端断言。

E2E：

```bash
scripts/run/run-case.sh E2E-01 --dataset smoke
scripts/run/run-e2e.sh E2E-01
```

覆盖：

| 用例 | 验证点 |
| --- | --- |
| E2E-01 | filter/column/aggregate 组合 transform 链 |
| E2E-02 | union → join → dedup |
| E2E-03 | validate/quarantine + schema contract |
| E2E-04 | PostgreSQL → JDBC 增量 upsert |
| E2E-05 | MySQL CDC → Paimon |
| E2E-06 | MySQL CDC → Hudi / Iceberg |
| E2E-07 | MongoDB CDC → Paimon |
| E2E-08 | PostgreSQL CDC → JDBC |
| E2E-09 | Kafka JSON → filter/column → Hudi |
| E2E-10 | Kafka Avro/Protobuf + Schema Registry → Paimon |
| E2E-11 | watermark、窗口聚合、stream-stream join、dedup |
| E2E-12 | Hudi → Paimon、Iceberg → Hudi 增量同步 |
| E2E-13 | Hive、Kafka、MongoDB、Elasticsearch round trip |
| E2E-14 | file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg full refresh |
| E2E-15 | 对象存储与 checkpoint/state 路径 |

### 3.5 执行矩阵、恢复和负向用例

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

结果：

- RG-01 kill -9 driver 恢复；
- RG-02 并发锁；
- RG-03 pending transaction abort/commit；
- RG-04 Kafka broker 重启续跑；
- RG-05 MySQL 重启后 CDC 续跑；
- RG-06 checkpoint 成功删除/超时保留；
- RG-07 state CLI；
- NG 11/11 PASS，错误信息不泄露凭据；
- OS-01/02/03/04/05/06/07/08 PASS；
- SR-11/12/13 CDC smoke PASS。

### 3.6 执行规模门禁

```bash
scripts/run/bench-case.sh E2E-04-scale --rows 10000002 \
  --result-case E2E-04-scale -- scripts/run/run-e2e.sh E2E-04-scale

scripts/run/bench-case.sh E2E-06-hudi-scale --rows 10000013 \
  --result-case E2E-06-hudi-mysql-cdc-scale -- scripts/run/run-e2e.sh E2E-06-scale

scripts/run/bench-case.sh E2E-09-scale --rows 10000000 \
  --result-case E2E-09-kafka-to-hudi-scale -- scripts/run/run-kafka-scale.sh
```

规模门禁：

| 用例 | 数据 | 断言 |
| --- | --- | --- |
| E2E-01 | 1.3 GiB CSV | transform 返回 10 行聚合结果 |
| E2E-14 Iceberg | 1.3 GiB CSV | 200 行目标原子替换为 10,000,000 行 |
| E2E-14 Hudi | 1.3 GiB CSV | 新建 10,000,000 行 Hudi 表 |
| E2E-14 Paimon | 1.3 GiB CSV | 新建 10,000,000 行 Paimon 表 |
| E2E-04 | 10M PostgreSQL | 全量 + 2 新增 + 1 更新 = 10,000,002 行 |
| E2E-06 Hudi | 10,000,013 行 MySQL CDC | Hudi 精确行数 |
| E2E-06 Iceberg | 10,000,013 行 MySQL CDC | Iceberg 精确行数 |
| E2E-09 | 10,000,000 Kafka JSON | Hudi 精确行数，重启无重复 |

### 3.7 采集指标

`bench-case.sh` 对每个用例：

- 记录 wall time；
- 开启 Spark event log；
- 采样 driver RSS；
- 采样 Docker 容器内存/CPU；
- 调用 `parse-spark-events.py` 提取 SQL/job 时间、task 数、输入字节、spill、shuffle；
- 汇总到 `results/bench/<case>-<timestamp>/metrics.json`。

汇总：

```bash
scripts/run/bench-summary.py results/bench
```

### 3.8 验证和清理

```bash
scripts/verify/table-check.sh hudi orders --count 10000013
scripts/verify/verify-case.sh E2E-06
scripts/run/run-capabilities.sh
```

验证通过后清理服务：

```bash
scripts/services/down.sh relational
scripts/services/down.sh streaming
scripts/services/down.sh search
scripts/services/down.sh infra
scripts/services/down.sh hdfs
```

## 4. 基准结果

| 用例 | 行数 | Wall | 有效完成 | Peak RSS | Spill | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| E2E-01 1.3 GiB transform | 10 输出行 | 23 s | 23 s | 1.05 GiB | 0 | PASS |
| E2E-14 Iceberg 1.3 GiB full refresh | 10,000,000 | 25 s | 25 s | 1.55 GiB | 0 | PASS |
| E2E-14 Hudi 1.3 GiB full refresh | 10,000,000 | 244 s | 244 s | 4.69 GiB | 0 | PASS |
| E2E-14 Paimon 1.3 GiB full refresh | 10,000,000 | 27 s | 27 s | 1.48 GiB | 0 | PASS |
| E2E-04 10M PostgreSQL incremental | 10,000,002 | 163 s | 146.3 s + 6.8 s | 4.77 GiB | 0 | PASS |
| E2E-06 10M MySQL CDC → Hudi | 10,000,013 | 4855 s | 3037 s | 8.68 GiB | 0 | PASS |
| E2E-06 10M MySQL CDC → Iceberg | 10,000,013 | 1021 s | 1021 s | 2.46 GiB | 0 | PASS |
| E2E-09 10M Kafka → Hudi | 10,000,000 | 1001 s | 195 s | 2.52 GiB | 0 | PASS |

说明：

- E2E-04 的 wall time 包含全量 bootstrap 和增量 2 insert/1 update 两个 Spark 应用；
- E2E-06 Hudi 数据完成时间按最后一个 Hudi commit 计算，3037 s 是有效完成时间，
  不是 runner 的 idle 超时时间；
- E2E-06 Iceberg 在 Iceberg `total-records` 达到 10,000,013 时停止，1021 s 为数据
  完成时间；
- E2E-09 的 195 s 从首个 Hudi commit 计算，不包含后续 idle 等待；
- 所有结果均为单机本地测量，不是生产 SLA。

## 5. 测试中发现并修复的问题

| 问题 | 影响 | 处理 |
| --- | --- | --- |
| 本地 client 静默成功 | 作业失败但进程返回 0 | 修复退出码和错误传播 |
| ES 连接器 Scala 2.12 不可用 | ES source/sink 运行时类加载失败 | 修复二进制兼容和依赖 |
| Hudi streaming append 空 microbatch | 0 行批次提交时报 `Positive number of partitions required` | 空批次只推进 checkpoint，不写 Hudi |
| CDC queueCapacity 自锁死 | 队列满后 Spark 不再提交上一批，poller 阻塞 | 容量至少为 `maxRecordsPerBatch + 1` |
| Hudi 10M CDC marker server 不可达 | 首次运行写入 6,200,031 行后失败 | 使用 `hoodie.write.markers.type=DIRECT` + `upsert` 从 checkpoint 续跑 |
| MinIO 镜像从 Docker Hub 移除 | 容器测试 404 | 改用 `quay.io/minio/minio` |

## 6. 已知限制

- 单机 Docker 环境，资源有限，不能代表生产集群；
- E2E-06 Hudi 10M 是长作业，默认 runner 超时 80 分钟；
- Hudi/Paimon 10M full refresh 新建表；Iceberg scale 需要预先建表；
- Hudi Jackson 编码的 DECIMAL 在 CDC 回放中不可恢复，产品选择 fail-closed；
- PostgreSQL inactive replication slot 可能保留大量 WAL，10M 写入前需要清理；
- 基准只覆盖当前主机和当前发行包，硬件、JDK、Docker 内存变化会影响结果；
- GitHub Actions 不执行完整基准，CI 只覆盖构建和容器回归。

## 7. 复现命令

一键执行完整基准：

```bash
scripts/run/bench-suite.sh all
```

也可以按阶段执行：

```bash
scripts/run/bench-suite.sh preflight
scripts/run/bench-suite.sh smoke
scripts/run/bench-suite.sh prepare
scripts/run/bench-suite.sh scale
scripts/run/bench-suite.sh report
```

手动分步复现：

```bash
cd /path/to/spark-pudding
mvn -B clean verify

cd /path/to/spark-pudding-benchmark
scripts/services/up.sh relational

python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/generated --formats csv,jsonl --force
scripts/data-gen/03_load_postgres.sh
scripts/data-gen/04_load_mysql.sh

scripts/run/run-smoke-suite.sh
scripts/run/run-case.sh E2E-01 --dataset smoke

scripts/services/down.sh relational
```

完整回归：

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

## 8. 结论

- 功能：P0 smoke 30/30 PASS，E2E-01 到 E2E-15 全部 PASS；
- 恢复：RG-01 到 RG-07 PASS；
- 负向：NG 11/11 PASS，错误信息不泄露凭据；
- 对象存储：OS-01/02/03/04/05/06/07/08 PASS；
- 规模：E2E-01/04/06/09/14 全部门禁 PASS；
- 构建：`mvn -B clean verify` 61 个模块 SUCCESS，0 失败。

本轮测试满足 `spark-pudding v1.0.0` 的本地回归发布条件。
