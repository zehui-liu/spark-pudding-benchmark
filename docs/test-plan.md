# spark-pudding 本地全链路测试方案

[English](test-plan.en.md) | 简体中文

| 项 | 内容 |
| --- | --- |
| 文档版本 | 1.0 |
| 编写日期 | 2026-11-06 |
| 适用代码 | spark-pudding `1.0.0`，分支 `v-1.0` |
| 测试床 | `spark-pudding-benchmark/`（本目录） |
| 测试方式 | 真实发行包 + `bin/pudding submit` + Docker Compose |
| 数据规模 | 所有主测试表（`orders`/`customers`/`payments`）单表 **≥ 1 GiB**（未压缩逻辑数据量） |

---

## 1. 目标

本方案用于在本地一次性验证 spark-pudding 的完整数据链路，优先发现数据正确性、状态一致性、
资源生命周期和错误处理方面的问题。

覆盖目标：

- **13 个 source 类型**（11 个模块，含 3 个 CDC source）；
- **11 个 sink 类型**；
- **10 个 transform 类型**；
- `batch`、`sync full`、`sync incremental`（append/upsert/cdc）、`streaming` 四种执行路径；
- checkpoint、增量游标状态、锁/fencing、失败恢复；
- 对象存储（S3A）、HDFS、湖仓表维护与能力清单；
- 正向链路、负面配置和故障注入；
- `orders`、`customers`、`payments` 三张主测试表，单表 ≥ 1 GiB 的数据规模。

不在本方案范围内：

- 生产集群、YARN/Kubernetes 多节点性能；
- 安全渗透测试；
- 与旧版本作业配置的兼容迁移；
- 长时间稳定性/浸泡测试（可作为后续独立计划）。

---

## 2. 总体架构

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

设计原则：

1. 使用**真实发行包**和统一 CLI，而不是只跑单元测试；
2. 外部依赖全部容器化，宿主机只运行 Spark；
3. 测试资产与产品代码分离，所有脚本、配置、数据都在 `spark-pudding-benchmark/`；
4. 小数据用于快速迭代，1 GiB 数据用于最终规模和正确性门禁；
5. 每个用例可重复执行，失败时保留最小复现所需配置和日志。

---

## 3. 测试床环境

### 3.1 当前环境

| 项 | 值 | 说明 |
| --- | --- | --- |
| CPU 架构 | arm64 | 大部分镜像有原生 arm64 |
| 宿主机内存 | 32 GiB | Spark 使用宿主机内存 |
| Docker 内存 | 约 3.8 GiB | **建议调到 8–12 GiB**，否则无法同时跑 ES/Kafka/DB |
| Docker 镜像/卷 | 当前约 9 GB | 建议 Docker 磁盘上限 ≥ 100 GB |
| 宿主机磁盘 | 可用约 158 GB | 三张 1 GiB 主表 + 多系统副本预计 55–75 GB，建议保留 ≥100 GB |
| JDK | Zulu 11 / 17（arm64） | 使用 11 或 17 |
| Spark | `/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3` | 与项目基线一致 |
| Python | 3.11 | 数据生成与加载脚本 |

### 3.2 Docker 服务与 profile

为避免 3.8 GiB Docker 内存被同时占满，按 profile 启停：

| profile | 服务 | 镜像 | 宿主机端口 | 用途 |
| --- | --- | --- | --- | --- |
| `relational` | postgres | `postgres:16-alpine` | 15432 | JDBC、PostgreSQL CDC |
| | mysql | `mysql:8.0` | 13306 | MySQL source/sink、binlog CDC |
| | mongo | `mongo:7.0` | 27018 | MongoDB source/sink、change stream |
| `streaming` | kafka | `apache/kafka-native:3.8.0` | 19092 | Kafka source/sink、streaming |
| | schema-registry | `confluentinc/cp-schema-registry:7.4.0` | 18081 | Avro/Protobuf |
| `search` | elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.9.1` | 19200 | ES source/sink |
| `infra` | minio | `quay.io/minio/minio:RELEASE.2023-09-04T19-57-37Z` | 19000/19001 | S3A checkpoint/state、Iceberg S3FileIO |
| | redis | `redis:7-alpine` | 16379 | Redis 增量状态 |
| | etcd | `quay.io/coreos/etcd:v3.5.9` | 12379 | etcd 增量状态 |
| `hdfs`（可选） | namenode/datanode | `apache/hadoop:3.3.6` | 18020 | HDFS checkpoint/state |

端口统一使用 1xxxx 段，避免与宿主机已有服务冲突。所有连接串统一放在
`config/env/` 下的环境模板中，不写死在 job 文件里。

### 3.3 服务关键参数

- **PostgreSQL**
  - `wal_level=logical`
  - `max_replication_slots=10`
  - `max_wal_senders=10`
  - 加载完成后对 `orders(updated_at)` 建索引，供增量游标使用。

- **MySQL**
  - `--server-id=1`
  - `--log-bin=mysql-bin`
  - `--binlog-format=ROW`
  - `--binlog-row-image=FULL`
  - JDBC URL 附加 `useSSL=false&allowPublicKeyRetrieval=true`。

- **MongoDB**
  - `--replSet rs0 --bind_ip_all`
  - 初始化：
    ```javascript
    rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27018"}]})
    ```
  - change stream 需要副本集，单节点副本集即可。

- **Kafka**
  - 双 listener：
    - 宿主机 Spark 使用 `PLAINTEXT://localhost:19092`
    - 容器内 Schema Registry 使用 `INTERNAL://kafka:29092`
  - 单节点 KRaft；
  - `auto.create.topics.enable=true` 仅用于测试；
  - 大topic 设置合理的 `retention.ms`/`retention.bytes`。

- **Elasticsearch**
  - `discovery.type=single-node`
  - `xpack.security.enabled=false`
  - `ES_JAVA_OPTS=-Xms1g -Xmx1g`（三张 1 GiB 表规模时建议 1–2g）
  - 单分片、0 副本，便于统计主存储大小。

- **Schema Registry**
  - arm64 下 `confluentinc/cp-schema-registry:7.4.0` 可能走 amd64 模拟；
  - 若过慢，改用宿主机 JDK 直接启动 Confluent Schema Registry 进程；
  - `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=INTERNAL://kafka:29092`。

### 3.4 目录规范

后续所有测试数据、脚本和配置都按以下目录放置：

| 内容 | 目录 |
| --- | --- |
| 测试方案、缺陷模板 | `docs/` |
| docker-compose、初始化 SQL、服务配置 | `config/docker/` |
| env.properties、state properties 模板 | `config/env/` |
| 各用例 job YAML/JSON/HOCON | `config/jobs/` |
| 表 DDL、Avro/Protobuf schema、ES mapping | `config/schemas/` |
| 小规模确定性种子数据 | `data/seed/` |
| 1 GiB 生成数据（可重建、不提交） | `data/generated/` |
| 数据生成与加载脚本 | `scripts/data-gen/` |
| compose 启停、健康检查、数据重置 | `scripts/services/` |
| 用例执行器 | `scripts/run/` |
| 数据/状态/checkpoint 断言 | `scripts/verify/` |
| 规模校验报告 | `results/data-scale/` |
| 每次用例的日志和结果 | `results/runs/<case-id>/` |
| 临时文件 | `tmp/` |

命名约定：

- 用例 ID：`SR-01`、`SK-03`、`TR-07`、`E2E-05`、`RG-02`、`NG-04`；
- job 文件：`config/jobs/<case-id>-<short-name>.yaml`；
- 结果目录：`results/runs/<case-id>/`；
- 每个结果目录至少包含：`job.yaml`、`env.properties`、`submit.log`、`assert.log`、`result.json`。

---

## 4. 数据集与 1 GiB 规模要求

### 4.1 计量口径

“单表至少 1G”统一按 **1 GiB = 1,073,741,824 bytes** 计算，并同时记录两种口径：

1. **逻辑数据量**：未压缩的行/文档/消息负载总大小；
2. **物理存储量**：数据库表空间、集合 `storageSize`、ES 主分片 store、Kafka log 目录、
   湖仓数据文件实际占用。

验收以逻辑数据量 ≥ 1 GiB 为准；物理存储量作为观测指标记录到
`results/data-scale/size-report.json`。对于列式湖仓，如果压缩后不足 1 GiB，规模基线写入时
关闭 Parquet 压缩（`parquet.compression=none`），确保物理数据文件也 ≥ 1 GiB。

### 4.2 逻辑数据模型

主测试表 `orders`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `order_id` | bigint | 主键，顺序生成 |
| `customer_id` | bigint | 1–1,000,000，用于 join |
| `amount` | decimal(12,2) | 确定性金额 |
| `status` | varchar(16) | `NEW/PAID/SHIPPED/DONE/CANCELLED` 循环 |
| `city` | varchar(32) | 20 个城市循环 |
| `order_time` | timestamp | 事件时间，递增 |
| `updated_at` | timestamp | 增量游标，递增 |
| `payload` | varchar(256) | 64 字符确定性填充，控制行宽 |

主测试表 2 `customers`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `customer_id` | bigint | 主键，顺序生成 |
| `name` | varchar(64) | 确定性姓名 |
| `city` | varchar(32) | 与 orders 的 city 取值一致 |
| `segment` | varchar(16) | `VIP/NORMAL/NEW` 循环 |
| `updated_at` | timestamp | 增量游标 |
| `payload` | varchar(256) | 96 字符确定性填充 |

主测试表 3 `payments`：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `payment_id` | bigint | 主键，顺序生成 |
| `order_id` | bigint | 关联 orders |
| `paid_amount` | decimal(12,2) | 金额 |
| `payment_time` | timestamp | 事件时间 |
| `updated_at` | timestamp | 增量游标 |
| `payload` | varchar(256) | 64 字符确定性填充 |

三张主测试表都按 **10,000,000 行、单表 ≥ 1 GiB** 生成。join 正确性用例可以通过
`filterExpr` 只读取其中 100 万行，避免每次回归都触发三表全量 shuffle；规模门禁用例必须至少
跑一次三表全量 join。

增量数据集（以 `orders` 为例）：

- `delta_insert`：50,000 行新订单；
- `delta_update`：200,000 行更新 `amount/status/updated_at`；
- `delta_delete`：20,000 行按 `order_id` 删除；
- `delta_late`：10,000 行迟到事件，用于 watermark 测试。

所有生成使用固定随机种子（默认 `42`），保证可重复。

### 4.3 行数与体积估算

基线行数：**三张主表各 10,000,000 行**。

| 系统 | 单表估算 | 三张主表合计 |
| --- | --- | --- |
| CSV | ~1.3–1.4 GB | ~3.9–4.2 GB |
| Parquet（不压缩） | ~1.1–1.2 GB | ~3.3–3.6 GB |
| PostgreSQL | ~1.5–2.5 GB | ~4.5–7.5 GB |
| MySQL | ~1.5–2.5 GB | ~4.5–7.5 GB |
| MongoDB | ~1.5–2.5 GB | ~4.5–7.5 GB |
| Elasticsearch | ~2–3.5 GB | ~6–10.5 GB |
| Kafka | ~1.3–1.75 GB | 按 topic 单独产生，合计约 4–5 GB |
| Hudi / Paimon / Iceberg | ~1.1–1.2 GB | 每套 ~3.3–3.6 GB |

说明：

- 三张主表都按 10M 行生成；若某系统物理大小已明显超过 1 GiB，可下调到 6M 行，但必须保证
  该表逻辑数据量 ≥ 1 GiB；
- 10M 行是体积估算值，最终以 `results/data-scale/size-report.json` 的实际测量为准；
- 如果磁盘不足，可以按“当前 profile 只需要一个系统”的方式顺序加载，但最终规模报告必须
  覆盖三张表在所有目标系统中的测量结果。

### 4.4 数据生成与加载

建议脚本划分（后续实现在 `scripts/data-gen/`）：

| 脚本 | 作用 |
| --- | --- |
| `01_generate_tables.py` | 流式生成 `orders`、`customers`、`payments` 的 CSV/JSONL |
| `02_generate_delta.py` | 生成增量 CSV/SQL |
| `03_load_postgres.sh` | 建表、`COPY` 三表、建索引、`ANALYZE` |
| `04_load_mysql.sh` | 建表、`LOAD DATA` 三表、建索引、`ANALYZE TABLE` |
| `05_load_mongo.py` | 批量 `insertMany` 三表，建立业务索引 |
| `06_produce_kafka.py` | 生产 orders JSON/Avro/Protobuf 主题 |
| `07_load_es.py` | 批量 `_bulk` 写入三表并 refresh |
| `08_build_lakehouse.sh` / `08_build_lakehouse.py` | 用 Spark 生成 Hudi/Paimon/Iceberg 三表基线 |
| `09_verify_sizes.sh` | 汇总三表在各系统的逻辑/物理大小，输出规模报告 |
| `10_merge_size_reports.py` | 合并分 profile 采集的规模报告，生成最终门禁报告 |

生成原则：

- 生成器必须流式写文件，不能把 10M 行放在内存；
- 先生成一次 `data/generated/`，后续所有加载复用；
- 数据加载脚本必须可重复执行：先 `DROP/TRUNCATE/DELETE`，再导入；
- 生成数据不提交到仓库，只保留生成脚本和校验和；
- 记录 `sha256` 与行数，便于确认各系统导入的是同一份数据。

加载命令示意：

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

### 4.5 体积校验

每个系统加载完成后必须执行：

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

以上命令对 `orders`、`customers`、`payments` 三张表分别执行，最终汇总为一份报告。
`09_verify_sizes.sh` 将结果写入：

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

三张主表在任一系统中的逻辑数据量低于 1 GiB 时，脚本返回非零，测试不能进入规模门禁。

### 4.6 双档数据

| 档位 | 行数 | 用途 | 位置 |
| --- | --- | --- | --- |
| `smoke` | 每表 100,000 | 日常开发、快速复现、每轮回归 | `data/seed/*-smoke.csv` |
| `scale` | 每表 10,000,000 | 最终验收、吞吐/内存/溢写、增量扫描范围 | `data/generated/*.csv` |

规则：

1. 所有组件正确性用例先在 `smoke` 上跑通；
2. `scale` 只跑每个连接器/执行模式的代表用例，不重复跑全部 34 个组件；
3. 发现 bug 后先在 `smoke` 上构造最小复现，再确认 `scale` 是否同样触发；
4. 规模结果单独记录，不与小数据结果混用。

### 4.7 磁盘、内存与时间预算

建议 Docker Desktop 配置：

- 内存：**8–12 GiB**（最低 8 GiB，三张 1 GiB 表同时加载时）；
- 磁盘：**≥ 100 GB**；
- CPU：8 核以上。

三张 1 GiB 主表的粗算磁盘占用（全部系统同时保留时）：

| 项 | 估算 |
| --- | --- |
| 原始 CSV/JSONL | 11–13 GB |
| PostgreSQL/MySQL/MongoDB | 14–22 GB |
| Kafka（三个 topic） | 4–5 GB |
| Elasticsearch | 6–10.5 GB |
| Hudi/Paimon/Iceberg（各一套三表） | 10–11 GB |
| MinIO 副本 | 3–4 GB |
| Docker 镜像 | ~10 GB |
| **合计** | **约 58–75 GB** |

因此建议：

- 宿主机至少保留 **100 GB** 可用磁盘；
- Docker Desktop 磁盘上限 **≥ 100 GB**；
- 不要同时长期保留所有系统副本；按 profile 顺序加载，测完一个系统后可删除其数据卷；
- 原始数据只保留一份，各系统通过加载脚本导入；
- 湖仓规模门禁可以按 Hudi → Paimon → Iceberg 顺序执行，每完成一个格式清理临时副本。

时间预算（一次性）：

| 步骤 | 预计 |
| --- | --- |
| 生成三张 1 GiB 原始数据 | 30–60 分钟 |
| PostgreSQL/MySQL 导入 + 索引（三表） | 每系统 20–40 分钟 |
| MongoDB 导入（三表） | 30–60 分钟 |
| Kafka 生产 | 10–30 分钟 |
| Elasticsearch 导入（三表） | 30–60 分钟 |
| 三种湖仓基线写入（每个格式三表） | 每格式 30–60 分钟 |
| 规模校验 | 20 分钟 |

如果 Docker 内存不足，优先只加载当前 profile 需要的系统；数据卷保留，后续 profile 复用。

---

## 5. 覆盖矩阵

### 5.1 Source（13 个类型）

| ID | type | 测试路径 | 关键断言 |
| --- | --- | --- | --- |
| SR-01 | `file-common-source` | CSV/JSON/Parquet/text → `file-sink` | 行数、schema、options、分区 |
| SR-02 | `jdbc-source` | PostgreSQL/MySQL 表 → `file-sink`；incremental append/upsert | 游标推进、二次运行增量为 0 |
| SR-03 | `mysql-source` | MySQL 表 → `file-sink` | 与 jdbc-source 结果一致 |
| SR-04 | `mongodb-source` | Mongo collection → `file-sink` | 文档数、字段映射、options |
| SR-05 | `elasticsearch-source` | ES index → `file-sink` | limit、字段类型、分页 |
| SR-06 | `kafka-source` | bounded Kafka JSON/raw → `file-sink` | starting/ending offsets、key/value |
| SR-07 | `hive-source` | 本地 Derby Hive 表 → `file-sink` | 条件过滤、schema projection |
| SR-08 | `hudi-source` | path/catalog snapshot + incremental → `file-sink` | 内部列过滤、增量游标 |
| SR-09 | `paimon-source` | path/catalog snapshot + incremental → `file-sink` | `_row_kind_`、scanMode |
| SR-10 | `iceberg-source` | path/catalog snapshot + incremental → `file-sink` | snapshot/branch/tag、增量 |
| SR-11 | `mysql-cdc-source` | MySQL binlog → Paimon/Hudi | insert/update/delete、op 列、顺序 |
| SR-12 | `postgres-cdc-source` | PostgreSQL logical decoding → Iceberg | 同上 |
| SR-13 | `mongodb-cdc-source` | Mongo change stream → Paimon/Iceberg | insert/update/delete、resume token |

### 5.2 Sink（11 个类型）

| ID | type | 测试路径 | 关键断言 |
| --- | --- | --- | --- |
| SK-01 | `console-sink` | CSV → console | stdout 行数、truncate/vertical |
| SK-02 | `file-sink` | CSV → parquet/json/csv | append/overwrite、分区、读回一致 |
| SK-03 | `jdbc-sink` | CSV → PostgreSQL/MySQL | append/overwrite/upsert/cdc、事务 |
| SK-04 | `mysql-sink` | CSV → MySQL | MySQL 方言、保留字、`ON DUPLICATE KEY` |
| SK-05 | `mongodb-sink` | CSV → Mongo | append/overwrite/update/upsert |
| SK-06 | `elasticsearch-sink` | CSV → ES | index 创建、idField、upsert |
| SK-07 | `kafka-sink` | CSV → Kafka | topic/key/value、consumer 校验 |
| SK-08 | `hive-sink` | CSV → Hive | saveAsTable、partitionBy、overwrite |
| SK-09 | `hudi-sink` | CSV → Hudi path/catalog | append/upsert/cdc/full refresh、幂等 |
| SK-10 | `paimon-sink` | CSV → Paimon path/catalog | 同上、`_row_kind_` |
| SK-11 | `iceberg-sink` | CSV → Iceberg path/catalog | 同上、MERGE、snapshot 顺序 |

### 5.3 Transform（10 个类型）

| ID | type | 测试内容 |
| --- | --- | --- |
| TR-01 | `filter-transform` | 条件过滤、`numPartitions` |
| TR-02 | `aggregate-transform` | 分组聚合；streaming window + watermark |
| TR-03 | `union-transform` | byName/byPosition、allowMissingColumns、distinct |
| TR-04 | `column-transform` | 选择/重命名/cast、未知字段报错 |
| TR-05 | `limit-transform` | limit=0/正常值/负值报错 |
| TR-06 | `join-transform` | inner/left/right/full/semi/anti；streaming inner join |
| TR-07 | `validate-transform` | drop/error/quarantine；streaming 仅 drop |
| TR-08 | `dedup-transform` | batch keys；streaming watermark |
| TR-09 | `schema-contract-transform` | 正常/缺列/类型不符/extra column |
| TR-10 | `sql-transform` | tempView/tempViews、多依赖、未知 view 报错 |

---

## 6. 用例设计

### 6.1 L0：构建与静态预检

| ID | 内容 | 通过标准 |
| --- | --- | --- |
| L0-01 | `mvn -B clean package -DskipTests` | 构建成功，发行包完整 |
| L0-02 | `bin/pudding capabilities verify "$PUDDING_HOME"` | 返回 0，jar 与 manifest 一致 |
| L0-03 | 静态扫描 `build/examples/*` | 每个示例有 `job.mode` 且恰好一个 sink |
| L0-04 | 示例必填字段检查 | `job.sparkHome` / `job.sparkPuddingHome` 存在或由 CLI 覆盖 |
| L0-05 | 占位符检查 | `${SPARK_HOME}`、`${PUDDING_HOME}` 已替换 |
| L0-06 | ServiceLoader / manifest / README 对比 | 34 个组件 identifier 一致 |

已知疑点（优先验证）：

1. `incremental_jdbc_to_jdbc.yaml`、`read_kafka_avro_example.yaml` 缺少
   `job.sparkHome` / `job.sparkPuddingHome`，而 client 在提交前要求这两个值；
2. client 报错提示 “set PUDDING_HOME”，但代码中没有读取 `PUDDING_HOME` 环境变量；
3. `build/examples/data/demo.csv` 表头 4 列、数据行 5 列；
4. README 声称内置 PostgreSQL/MySQL JDBC driver，但 `libs/` 中只有 `cdc-runtime` 携带
   driver，普通 `jdbc-source`/`jdbc-sink` 可能拿不到；
5. Schema Registry 镜像在 arm64 下需要 amd64 模拟。

### 6.2 L1：Source 单组件

执行方式：

- 非 CDC：source → `file-sink`（JSON），用 `jq`/Python 校验；
- CDC：source → Paimon/Iceberg，按第 6.5 节流式流程验证；
- 每条用例先在 `smoke` 数据上跑，规模门禁再跑 `scale`。

断言：

- 行数/文档数；
- 关键字段抽样值与校验和；
- schema 字段名、类型、可空性；
- 增量用例第二次运行新增行数为 0，cursor 不变；
- 日志中不出现连接串密码、secret 值。

### 6.3 L2：Sink 单组件

执行方式：固定 `smoke` CSV → 每个 sink → 读回。

读回方式：

- JDBC/MySQL：`psql`/`mysql` 直接查询；
- Mongo：`mongosh` 查询；
- ES：`curl _search`；
- Kafka：`kafka-console-consumer` 或 `kafka-console-consumer.sh`；
- Hive：`spark-sql` 或 `hive-source → file-sink`；
- Hudi/Paimon/Iceberg：对应 source → file-sink，并额外用表路径文件大小/metadata 校验；
- File/Console：直接读文件/stdout。

重点断言：

- append 不覆盖旧数据；
- overwrite/full refresh 只保留新快照；
- upsert 后每个 key 只有一行且为最新值；
- CDC delete 真正删除目标行；
- 重复执行同一批次结果幂等；
- 失败后目标表没有半提交数据。

### 6.4 L3：Transform 单组件

| ID | 输入 | 期望 |
| --- | --- | --- |
| TR-01 | 订单 CSV，`amount >= 100` | 行数与 SQL 结果一致 |
| TR-02 | 按 `city` 聚合 count/sum | 分组结果一致；streaming 按 watermark 输出 |
| TR-03 | 两份同构 CSV | byName/byPosition 行数、列顺序正确 |
| TR-04 | 重命名 + cast | 列名/类型正确；未知字段报错 |
| TR-05 | limit 0/10 | 行数正确；负数报错 |
| TR-06 | orders + customers | 6 种 join 类型；streaming 仅 inner |
| TR-07 | 含非法金额/空 key 数据 | drop/error/quarantine 行为正确 |
| TR-08 | 重复 `order_id` | 每个 key 一行；streaming 受 watermark 限制 |
| TR-09 | 正常/缺列/多列/类型变化 | 按契约通过或失败 |
| TR-10 | 单/多依赖 SQL | 视图解析、结果正确；未知 view 报错 |

负面用例必须断言：

- 失败发生在 Spark 启动前或首个 action 前（能提前校验的）；
- 错误信息包含配置 key 名，不包含敏感 value；
- 退出码非 0。

### 6.5 L4：模式与端到端金路径

| ID | 流程 | 模式 | 断言 |
| --- | --- | --- | --- |
| E2E-01 | CSV → filter → column → aggregate → limit → file | batch | 组合顺序、结果与 SQL 基线一致 |
| E2E-02 | CSV + CSV → join → union → dedup → file | batch | 多依赖 DAG、join key 合并 |
| E2E-03 | CSV → validate(quarantine) + schema-contract → file | batch | 坏行进入 quarantine，好行继续 |
| E2E-04 | PostgreSQL → JDBC incremental upsert → PostgreSQL | sync incremental | 新增/更新/删除、重放幂等 |
| E2E-05 | MySQL → MySQL CDC → Paimon | streaming | insert/update/delete、op 列 |
| E2E-06 | MySQL CDC → Hudi；MySQL CDC → Iceberg | streaming | 同一数据三种湖仓行为一致 |
| E2E-07 | MongoDB CDC → Paimon/Iceberg | streaming | resume token、删除 |
| E2E-08 | PostgreSQL CDC → JDBC sink | streaming | 单事务 delete + upsert |
| E2E-09 | Kafka JSON → filter/column → Hudi update | streaming | checkpoint、重启不重不漏 |
| E2E-10 | Kafka Avro/Protobuf → Paimon/Iceberg | streaming | Schema Registry 解码、subject 策略 |
| E2E-11 | Kafka → window aggregate / dedup / stream-stream join → Paimon | streaming | watermark、迟到数据、状态清理 |
| E2E-12 | Hudi → Paimon；Iceberg → Hudi | sync incremental | 湖仓间增量同步 |
| E2E-13 | Hive ↔ file；ES ↔ Mongo；Kafka sink → Kafka source | batch | 双向链路、字段映射 |
| E2E-14 | file → file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg 的 `sync full` | sync full | 全量替换、旧数据清除；Paimon/File/Hive 需显式 `allowNonAtomicReplace` |
| E2E-15 | S3A checkpoint/state + Iceberg S3FileIO + checkpoint CLI | streaming/sync | MinIO 路径、权限、清理 |

规模门禁至少覆盖：

- E2E-01（大文件与 transform）；
- E2E-04（1 GiB 全量 + 增量游标）；
- E2E-05 或 E2E-06（CDC 初始快照 1 GiB）；
- E2E-09（Kafka 1 GiB 流式）；
- E2E-14（湖仓 1 GiB 全量替换）。

### 6.6 L5：故障、恢复与负面用例

| ID | 场景 | 断言 |
| --- | --- | --- |
| RG-01 | incremental 执行中 `kill -9` driver，然后重跑 | 不重复、不丢数据 |
| RG-02 | 两个进程同时对同一 source id 运行 | 只有一个拿到锁，另一个明确失败 |
| RG-03 | source 读完后 sink 不可用 | 留下 pending transaction；`state resolve --abort/--commit` 可恢复 |
| RG-04 | Kafka broker 重启 | checkpoint 续跑，offset 不丢 |
| RG-05 | MySQL/PostgreSQL 重启 | CDC 从 binlog/LSN 恢复 |
| RG-06 | `delete-on-success` + 外部 `query.stop()` | checkpoint 保留，不误删 |
| RG-07 | `state reset/unlock/migrate` | 游标、锁、JDBC 状态表行为正确 |
| NG-01 | 多个 sink | 启动前拒绝 |
| NG-02 | transform 依赖缺失/环 | 明确报错 |
| NG-03 | source/sink 不支持目标 mode | 启动前拒绝 |
| NG-04 | 非法 outputMode | 明确报错 |
| NG-05 | 缺少必填 config | 错误信息只包含 key，不包含 value |
| NG-06 | 重复 key / 非法 op 列 | sink 拒绝 |
| NG-07 | schema 漂移 | 按契约失败或按策略处理 |
| NG-08 | `${secret:...}` 缺失/错误 | 日志和异常不泄露凭据 |

故障注入方式：

- `docker stop/start`：数据库、Kafka、ES 重启；
- `docker pause/unpause`：网络/进程暂停；
- `kill -9`：直接杀 driver JVM；
- 端口占用/错误密码：连接失败路径；
- 修改 checkpoint 目录权限：IO 失败路径。

### 6.7 L6：对象存储、HDFS 与维护

| ID | 内容 | 断言 |
| --- | --- | --- |
| OS-01 | S3A checkpoint + filesystem state（MinIO） | 写入/读取/清理正确 |
| OS-02 | Iceberg S3FileIO warehouse | 与 S3A 配置分离，凭据不混用 |
| OS-03 | HDFS checkpoint/state | NameNode/DataNode 读写、恢复 |
| OS-04 | `checkpoint show/status/delete/cleanup` | JSON 字段、dry-run、年龄计算 |
| OS-05 | `state show/status/resolve/unlock/migrate` | 状态一致、锁释放 |
| OS-06 | `maintenance iceberg/paimon/hudi` | status/compact/expire/clean，`--confirm`/`--dry-run` |
| OS-07 | `maintenance-server` | token、幂等、取消、审计文件 |
| OS-08 | `capabilities verify/show/diff` | 版本差异、缺失组件检测 |

---

## 7. 执行流程

### 7.1 阶段划分

| 阶段 | 内容 | 预计 |
| --- | --- | --- |
| 阶段 0 | 环境准备、compose、构建发行包、L0 静态检查 | 0.5 天 |
| 阶段 1 | `smoke` 数据：13 source + 11 sink + 10 transform | 2–3 天 |
| 阶段 2 | `smoke` 数据：E2E 金路径、四种模式 | 2–3 天 |
| 阶段 3 | 三张 1 GiB 表生成/加载、规模门禁用例 | 1–2 天加载 + 2–3 天执行 |
| 阶段 4 | 故障恢复、负面用例、对象存储/维护 | 2 天 |
| 阶段 5 | 回归、缺陷复验、结果汇总 | 1–2 天 |

全量执行预计 8–12 个工作日；如果按 profile 顺序加载并只跑 P0，约 5–7 个工作日。

P0 优先级（先跑）：

1. L0 全部；
2. SR-01/02/06/11、SK-02/03/07/09/10/11、TR-01/02/06/07/09；
3. E2E-01/04/05/09/14；
4. RG-01/02/03、NG-01–NG-08；
5. 1 GiB 规模门禁中的 E2E-04/E2E-09/E2E-14。

P0 的自动化入口：

```bash
scripts/run/bench-suite.sh all
```

也可以分阶段执行 `preflight`、`smoke`、`prepare`、`scale`、`report`。完整报告写入
`results/bench/suite-<run-id>/report.md`，并复制到 `results/bench/benchmark-report.md`。

### 7.2 单用例执行流程

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

### 7.3 数据生命周期

- `smoke` 数据：每次运行前可重建，秒级~分钟级；
- `scale` 数据：生成一次，保留在 `data/generated/` 和 Docker volume 中；
- 每个用例只重置自己使用的目标表/目录，不清理其他人的数据；
- 需要彻底重置时使用 `scripts/services/reset-all.sh`，并重新加载；
- 测试结束后保留 `results/` 与 `data/generated/` 的校验和，删除大文件由人工决定。

---

## 8. 断言与证据

### 8.1 结果目录

```text
results/runs/<case-id>/
  job.yaml
  env.properties
  submit.log
  spark.log
  assert.log
  result.json
```

`result.json` 至少包含：

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

### 8.2 断言维度

1. **退出码**：前台 `pudding submit` 只有 `FINISHED` 返回 0；
2. **数据**：行数、字段、校验和、抽样比对；
3. **幂等**：同一批次重复执行两次结果一致；
4. **增量范围**：第二次运行只扫描增量，不重复全表；
5. **恢复**：kill/重启后的最终结果与不中断运行一致；
6. **资源**：连接、临时文件、checkpoint、状态锁正确释放；
7. **安全**：日志/异常不出现密码、secret 值、完整连接串；
8. **规模**：`results/data-scale/size-report.json` 全部 ≥ 1 GiB。

### 8.3 独立校验

避免只用 spark-pudding 自己验证自己：

- 关系库用 `psql`/`mysql`；
- MongoDB 用 `mongosh`；
- ES 用 `_search`/`_count`；
- Kafka 用 console consumer；
- 文件用 `wc`/`sha256sum`/Python；
- 湖仓优先用原生 Spark SQL 读取；至少保留一条 `source → file` 的交叉校验。

---

## 9. 缺陷管理

### 9.1 严重级别

| 级别 | 定义 | 示例 |
| --- | --- | --- |
| P0 | 数据丢失、重复、错误结果、静默成功、凭据泄露 | upsert 后旧值覆盖新值；失败但退出码 0 |
| P1 | 功能不可用、恢复失败、明确报错但无法继续 | CDC 无法启动；checkpoint 无法恢复 |
| P2 | 边界行为、错误信息、文档/示例问题 | 示例缺少必填字段；提示信息误导 |
| P3 | 体验、性能、非阻塞告警 | 日志过多、UI 指标缺失 |

### 9.2 报告模板

每条缺陷记录：

- 标题：`[组件] 简短现象`
- 环境：分支/commit、JDK、Spark、发行包路径、Docker 镜像 tag；
- 数据档位：`smoke` / `scale`，行数；
- 配置：对应 `config/jobs/<case>.yaml`（脱敏）；
- 步骤：最小复现命令；
- 期望/实际；
- 证据：`results/runs/<case-id>/` 路径、关键日志片段；
- 影响：数据正确性/恢复/性能/安全；
- 初步定位：source/sink/transform/core/runtime。

缺陷修复后必须：

1. 在 `smoke` 上回归；
2. 在触发缺陷的 `scale` 用例上复验；
3. 更新 `results/` 中的状态；
4. 补充一个自动化断言，防止回归。

---

## 10. 风险与前置检查

### 10.1 环境风险

| 风险 | 缓解 |
| --- | --- |
| Docker 内存不足 | 调到 8–12 GiB；按 profile 启停；ES/Kafka 单独跑 |
| Docker 磁盘不足 | 设置 ≥100 GB；只保留当前 profile 的数据卷 |
| arm64 镜像缺失 | Schema Registry/HDFS 使用 amd64 模拟或宿主机进程 |
| Kafka advertised listener 配错 | 宿主机用 `localhost:19092`，容器内用 `kafka:29092` |
| Mongo 副本集 host 不一致 | 初始化时使用 `localhost:27018` |
| MySQL 认证插件 | URL 加 `allowPublicKeyRetrieval=true&useSSL=false` |
| 端口冲突 | 统一使用 15432/13306/27018/19092/18081/19200 等端口 |
| macOS 无 GNU timeout | 安装 coreutils 或使用 `gtimeout`/Python 超时 |

### 10.2 数据风险

- 1 GiB 数据生成/加载时间长，必须缓存，不能每个用例重建；
- 数据生成必须固定 seed，保证可复现；
- 时间字段统一 UTC，避免时区和夏令时干扰；
- 金额使用 `decimal`，避免浮点误差；
- 删除/更新增量必须有明确的 id 列表，便于计算期望结果；
- 大数据断言不能用 `collect()`，使用 count/sum/checksum/抽样。

### 10.3 已知前置检查

在启动 Docker 前先完成：

1. 构建发行包并 `capabilities verify`；
2. 静态检查所有示例的必填字段和占位符；
3. 用 `smoke` 数据跑一个最小 `file → file` 作业，确认 Spark、JDK、发行包路径正确；
4. 用 `pudding submit` 跑一个不带额外 JDBC driver 的 `jdbc-source`，确认 driver 是否真内置；
5. 确认 Docker Compose 的 Kafka/Schema Registry 在 arm64 下可启动；
6. 确认 Docker 磁盘上限 ≥ 100 GB，再开始 1 GiB 数据加载。

---

## 11. 后续落地清单

按顺序实现：

1. `config/docker/docker-compose.yml` + 初始化 SQL；
2. `config/env/*.properties` 模板；
3. `scripts/services/`：启停、健康检查、重置；
4. `scripts/data-gen/`：smoke/scale 生成、各系统加载、体积校验；
5. `config/jobs/`：34 个组件的最小 job + 15 条 E2E job；
6. `scripts/run/run-case.sh`：统一执行器；
7. `scripts/verify/`：行数、校验和、状态、checkpoint、日志断言；
8. `scripts/services/reset-data.sh`；
9. `docs/bug-report-template.md` 与结果汇总模板；
10. 先跑 P0，再决定是否扩展到完整 1 GiB 门禁。

### 11.1 当前实现状态（2026-11-06）

- ✅ `config/docker/docker-compose.yml`：relational/streaming/search/infra/hdfs
  profiles、初始化 SQL、健康检查；
- ✅ `scripts/services/up.sh`、`down.sh`、`status.sh`、`init-mongo.sh`、
  `reset-data.sh`；
- ✅ `scripts/data-gen/01_generate_tables.py`、`02_generate_delta.py`，
  已用 smoke 数据验证 CSV/JSONL 格式和行数；
- ✅ `03_load_postgres.sh`、`04_load_mysql.sh`、`05_load_mongo.py`，
  已用 smoke 数据完成加载；
- ✅ `06_produce_kafka.py` 的 JSON、Avro、Protobuf，已在本地 Kafka +
  Schema Registry 验证；
- ✅ `07_load_es.py`，已在本地 Elasticsearch 验证；
- ✅ `08_build_lakehouse.sh` / `08_build_lakehouse.py`，Hudi/Paimon/Iceberg
  各用 1000 行完成基线写入；
- ✅ `09_verify_sizes.py` / `09_verify_sizes.sh`，已验证 1 GiB 阈值失败路径；
- ✅ 1 GiB scale 数据的完整加载与门禁：
  `results/data-scale/size-report.json` 中 `passed=true`，无 failure/missing；
- ✅ 34 个组件 job 模板（`config/jobs/`）；
- ✅ `scripts/run/` 用例执行器与基础输出断言；
- ✅ P0 smoke：30 个有限批处理用例，30 PASS，0 FAIL；
- ✅ Elasticsearch 连接器 P0 缺陷已修复（Scala 2.12 运行时）：
  `results/runs/ES-connector-bug.md`；
- ✅ 本地 client 静默成功 P0 缺陷已修复：
  `results/runs/client-exit-code-bug.md`；
- ✅ sink 目标端断言：`scripts/verify/verify-case.sh`；SK-09/10/11 追加了
  Spark 读表行数断言（`scripts/verify/table-check.sh` + `paimon-check.py`）；
- ✅ E2E-01 组合 transform 链路 PASS；
- ✅ E2E-02 union → inner join → projection → dedup PASS（15,384 行）；
- ✅ E2E-03 validate(drop+quarantine) + schema-contract PASS
  （7 行有效、3 行进入隔离）；
- ✅ E2E-08 PostgreSQL CDC → JDBC sink：快照、delete + upsert 单事务、
  checkpoint 重启 PASS；
- ✅ E2E-07 MongoDB CDC → Paimon：快照、按 `_id` 删除、upsert、
  resume token、checkpoint 重启 PASS；
- ✅ E2E-06 MySQL CDC → Hudi/Iceberg：快照、upsert、delete、
  checkpoint 重启 PASS；
- ✅ CDC smoke：SR-11（MySQL）、SR-12（PostgreSQL，10,000 行初始快照）、
  SR-13（MongoDB，10,000 行快照 + I/U/D）PASS；
  `results/runs/cdc-smoke-summary.md`；
- ✅ E2E-04 PostgreSQL → JDBC incremental upsert → PostgreSQL PASS
  （初始全量、2 行新增、1 行更新、无变更续跑、重放幂等）；
- ✅ E2E-05 MySQL CDC → Paimon PASS
  （快照、upsert、delete、checkpoint 重启、重启后继续增量）；
- ✅ E2E-09 Kafka JSON → filter/column → Hudi PASS
  （消费 10,000 条、checkpoint 重启后消费 3 条增量、upsert 更新、无重复）；
- ✅ `scripts/run/run-e2e.sh`：多步 E2E 编排与进程组超时清理；
- ✅ `scripts/run/run-object-store.sh`：MinIO 夹具、S3A checkpoint/state、
  Iceberg S3FileIO 与 checkpoint CLI 的对象存储编排；
- ✅ `scripts/run/run-hdfs.sh`、`run-state-backends.sh`、`run-maintenance.sh`、
  `run-maintenance-server.sh`、`run-capabilities.sh`：OS-03/05/06/07/08
  编排；
- ✅ `scripts/run/run-join-e2e.sh`、`run-schema-registry-subject.sh`：
  E2E-11 自 join 与 Schema Registry subject/错误路径编排；
- ✅ E2E-14 `sync full` 七种 sink：file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg
  各跑 100 行后再替换为 200 行，均 PASS；
- ✅ RG-01 增量写入中 `kill -9` driver：留下 pending transaction，
  `state unlock` 后重跑，目标 10,000 行、无重复、pending 清除；
- ✅ RG-02 并发增量任务：第二个任务被状态锁拒绝，第一个任务写入
  200,000 行后正常提交；
- ✅ RG-03 sink 不可用留下 pending transaction：`resolve --abort` 后
  重放写入 10,000 行；`resolve --commit` 跳过未写入批次，目标保持 0；
- ✅ RG-04 Kafka broker 重启：首 10,000 条消费后重启 broker，再写入
  2 条；checkpoint 续跑，输出 10,002 条且无重复；
- ✅ RG-05 MySQL 重启后 CDC 续跑：重启前/后两行均捕获；Debezium binlog
  client 约 2 分钟后自行重连，见 `results/runs/recovery-summary.md`；
- ✅ RG-07 `state status/show/reset/unlock/migrate/list`：JSON 字段、
  游标重置与恢复、无锁提示、filesystem target 拒绝 migrate；
- ✅ 负面用例 NG-01/02/03/04/05/07/08：多 sink、未知依赖、流式模式不支持、
  非法 outputMode、缺失 `url`、schema 缺列、缺失 secret 均在启动前失败，
  错误信息只包含 key 名，不泄露凭据
  （`results/runs/negative-summary.md`）；
- ✅ 规模门禁 E2E-01：1.3 GiB `orders.csv` 上完成 filter/column/aggregate/
  limit，输出 10 行；
- ✅ 规模门禁 E2E-04：10,000,000 行 PostgreSQL → JDBC 初始全量，再新增
  2 行、更新 1 行；目标端 10,000,002 行。scale 模板使用 8g driver heap、
  32 个 shuffle 分区并关闭 AQE；`JdbcIncremental` 已改为按业务 key
  repartition，避免单 task 写入 OOM；
- ✅ E2E-10 Kafka Avro/Protobuf + Schema Registry → Paimon：
  两种编码各 10,000 条，通过 Confluent wire format 解码并写入 Paimon；
- ✅ E2E-11 有状态流式：5 分钟窗口聚合（1 分钟 watermark）关闭 4 个窗口、
  合计 600 条事件；dedup 对 1,000 唯一 key + 100 重复输出 1,000 行；
- ✅ E2E-12a 湖仓增量同步：1,000 行 Hudi → Paimon 初始全量，Hudi
  upsert 2 新增 + 1 更新后第二次 sync 只读新 commit，Paimon 目标
  1,002 行；
- ✅ E2E-12b Iceberg → Hudi：1,000 行全量后追加 2 行，第二次 sync
  读取新 snapshot，Hudi 目标 1,002 行；
- ✅ E2E-13 跨系统 round trip：Hive、Kafka、MongoDB、Elasticsearch
  各写入 100,000 行并读回 100,000 行；ES 阶段在停止 Kafka/Mongo 后单独
  执行（Docker Desktop VM 3.8 GiB，ES 1g heap 仍会被 OOM kill）；
- ✅ E2E-15 / OS-01 / OS-02 / OS-04 对象存储：
  S3A checkpoint 经过重启与新增文件仍不重复（10 → 10 → 12 行）；
  S3A filesystem state 在未设置 `allowBestEffortLock` 时被拒绝，开启后
  100 → 102 行且无重复；`state status` 显示已提交游标、无 pending、无锁；
  Iceberg JDBC catalog + S3FileIO 写入并读回 1,000 行；
  受限 MinIO 用户验证 S3A 凭据不能读 warehouse、S3FileIO 凭据不能读
  checkpoint，错误凭据写入返回 403；checkpoint CLI 的 JSON 字段、
  dry-run、`--keep-last`、`--older-than`、非法参数与安全拒绝均通过
  （`results/runs/object-store-summary.md`）；
- ✅ RG-06 checkpoint 保留语义：`delete-on-success` + `available-now`
  自然完成后删除 checkpoint（10 行）；3 秒 `timeoutMs` 外部停止时
  checkpoint 保留，并记录 “did not prove that its input was drained”；
- ✅ NG-02 依赖环：两个 transform 互相依赖时在启动前报
  `transform dependencies contain a cycle`；
- ✅ NG-06 streaming CDC 校验：非法 op 值、缺失 op 列、缺少排序字段
  均在查询执行时失败，错误信息包含列名和非法值；
- ✅ E2E-14 Iceberg 1.3 GiB `sync full` 规模用例：既有 200 行目标被
  原子替换为 10,000,000 行（commit 指标 `removedRecords=200`、
  `addedRecords=10000000`）；
- ✅ OS-03 HDFS：checkpoint 10 → 10 → 12 行；NameNode/DataNode 重启后
  checkpoint 与增量状态均可恢复（50 → 51 行，无重复）；
- ✅ OS-05 state 后端：JDBC/Redis/etcd 各 50 → 51 行，fencing token
  递增、锁释放；JDBC `state migrate` 幂等、reset/restore 正确；
- ✅ OS-06 表维护：Iceberg/Paimon/Hudi 的 status、compact/rewrite、
  expire、orphan、archive 以及 `--confirm`/`--dry-run` 与
  CopyOnWrite/path-only 拒绝；
- ✅ OS-07 maintenance-server：token 鉴权、幂等、排队取消、完成任务
  拒绝取消、确认门禁、audit 不含 token；
- ✅ OS-08 capabilities：verify/show/diff、缺失 jar 与缺失 manifest；
- ✅ Hudi streaming append 空 microbatch：修复
  `Positive number of partitions required`，由 10M 行 CDC 快照覆盖；
- ✅ CDC 队列容量自锁死：`queueCapacity == maxRecordsPerBatch` 时队列
  填满后上一批永不提交、poller 阻塞；实际容量现在至少为
  `maxRecordsPerBatch + 1`；
- ✅ E2E-06 MySQL CDC 10,000,013 行初始快照 → Hudi：首次运行写入
  6,200,031 行后因本机 Hudi HTTP marker server 地址不可达失败；从
  checkpoint 使用 `hoodie.write.markers.type=DIRECT` + `upsert` 续跑，
  最终 Hudi 表精确为 10,000,013 行，快照重放幂等；
- ✅ E2E-09 Kafka 10,000,000 条 JSON 流式规模用例：4 分区
  `orders-scale` topic，生产约 301k msg/s；Hudi bulk insert 最终
  10,000,000 行、484 MB（`results/runs/kafka-scale-summary.md`）；
- ✅ E2E-11 自 join：同一 Kafka source 派生 PAID 左右流，按 `order_id`
  inner stream-stream join，双侧 1 分钟 watermark；首跑 5 行，checkpoint
  重启后 0 重复（`results/runs/join-e2e-summary.md`）；
- ✅ Schema Registry subject/错误路径：自定义 subject 解码 1,000 行；
  wire format 的 schema id 优先，缺失 subject 不影响 id-based 消息；
  未知 schema id 999999 失败且错误包含 topic/id
  （`results/runs/schema-registry-subject-summary.md`）；
- ✅ 完整 `mvn clean verify`（JDK 11）：61 个模块全部 SUCCESS，
  0 失败（2026-11-08；含宿主休眠共 7h44m）；
- ✅ 测试方案中的规模门禁全部完成：E2E-01/04/06/09/14。

落地过程中确认的配置要求：

- Spark/Hudi 必须使用 **arm64 JDK 11 或 17**；默认 shell 的 JDK 8 会导致
  `spark-class` 找不到 Java。湖仓脚本支持 `PUDDING_JAVA_HOME` 覆盖。
- MySQL 容器不能把只读 `/data` 用作 `--secure-file-priv`，否则 entrypoint 在
  `chown` 时失败；当前使用 `--secure-file-priv=` 关闭目录检查。
- Schema Registry 的 `kafkastore.bootstrap.servers` 要使用
  `PLAINTEXT://kafka:29092`，不能直接使用自定义 listener 名 `INTERNAL://`。
- MongoDB 使用容器内端口 27018，保证副本集成员 `localhost:27018` 在容器内外
  都可解析；副本集必须在容器启动后由 `init-mongo.sh` 初始化。
- Hudi 需要 `spark.serializer=KryoSerializer`，非分区表需要
  `NonpartitionedKeyGenerator` 和空的 `partitionpath.field`。
- Paimon 需要 `spark.sql.extensions=PaimonSparkSessionExtensions` 和
  `spark.sql.catalog.paimon`；基线表使用主键 + bucket=4。
- Paimon 1.3.2 的 `file.compression=none` 会触发
  `CompressionCodecName.NONE` 异常（Parquet 枚举只有 `UNCOMPRESSED`），因此
  Paimon 基线使用默认压缩；规模门禁按未压缩源 CSV 的逻辑大小计量，文件系统
  物理大小单独记录。
- Iceberg 基线使用 Hadoop catalog；`write.parquet.compression-codec` 必须用
  `uncompressed`，`none` 会被拒绝。
- 本机 `HADOOP_CONF_DIR` 的 `fs.defaultFS=hdfs://localhost:9000` 与 MinIO 的
  9000 端口冲突；本地 state/checkpoint 路径不带 scheme 时会被 Hadoop 解析到
  默认 HDFS，报 `RPC response exceeds maximum data length`。测试脚本默认
  `unset HADOOP_CONF_DIR`，只有 HDFS 用例设置 `PUDDING_USE_HADOOP_CONF=1`。
- HDFS 容器的 named volume 由 root 创建、进程以 `hadoop` 运行，首次启动前需
  对 `/tmp/hadoop-root` 执行 `chown -R hadoop:hadoop` 并运行
  `hdfs namenode -format`；宿主机客户端需设置
  `dfs.client.use.datanode.hostname=true`，否则会拿到容器 IP
  （`172.21.x.x:19866`）。NameNode 重启后要等 `dfsadmin -safemode wait`
  退出安全模式再提交作业。
- Docker Desktop 默认 VM 内存约 3.8 GiB；同时运行 Kafka、Schema Registry、
  MongoDB、PostgreSQL、MySQL 时，ES 即使降到 1g heap 仍会被 OOM kill
  （exit 137）。E2E-13 的 ES 阶段先停止 Kafka/Mongo 再执行，或调大 Docker
  VM 内存。
- Paimon 的 `supportsAtomicReplace=false`（替换不是单个 staged commit），因此
  `sync full` 必须显式设置 `allowNonAtomicReplace=true`；catalog 模式可用
  `saveAsTable` 建表，但不会自动创建 namespace，需要先
  `CREATE DATABASE`；path 模式的 overwrite 不会自动建表，需要先创建表。

已完成的规模测量（单位 GiB，逻辑数据量）：

| 系统 | orders | customers | payments | 结果 |
| --- | --- | --- | --- | --- |
| CSV | 1.321 | 1.450 | 1.197 | 通过 |
| PostgreSQL | 1.871 | 2.142 | 1.830 | 通过 |
| MySQL | 1.992 | 1.784 | 1.679 | 通过 |
| MongoDB | 2.395 | 2.326 | 2.114 | 通过 |
| Kafka（orders topic） | 2.791 | — | — | 通过 |
| Hudi | 1.321 | 1.450 | 1.197 | 通过（物理 1.43/1.71/1.43） |
| Paimon | 1.321 | 1.450 | 1.197 | 通过（物理 0.37/0.31/0.35） |
| Iceberg | 1.321 | 1.450 | 1.197 | 通过（物理 1.02/1.30/1.01） |
| Elasticsearch | 1.617 | 1.237 | 1.463 | 通过 |

验收完成标志：

- 34 个组件全部至少有一条通过记录；
- 15 条 E2E 金路径全部至少跑过一次；
- `results/data-scale/size-report.json` 中所有主测试表 ≥ 1 GiB；
- 负面/恢复用例有明确通过或已知缺陷记录；
- 所有失败项都有最小复现和缺陷编号。
