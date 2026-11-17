# spark-pudding-benchmark

[English](README.en.md) | 简体中文

`spark-pudding` 的本地单节点全链路测试与基准项目，用于验证 `v1.0.0` 发布版本在真实
Spark、JDBC、CDC、Kafka、湖仓和对象存储组合下的正确性、恢复能力和规模表现。

当前配套版本：

```text
spark-pudding  v1.0.0
本项目版本      1.0.0
开发版本       1.0.1-SNAPSHOT
```

## 版本同步策略

本项目版本与 `spark-pudding` 发布 tag 保持一致。

| spark-pudding | spark-pudding-benchmark |
| --- | --- |
| `v1.0.0`（当前 tag） | `1.0.0` |
| `1.0.1-SNAPSHOT`（开发线） | `1.0.1-SNAPSHOT` |
| `v1.0.1`（下一个 tag） | `1.0.1` |

规则：

- 当前以最新发布 tag 为准：`v1.0.0` 对应本项目 `1.0.0`；
- `spark-pudding` 开发版本进入下一个版本时，本项目同步 bump；
- 发布时去掉 `-SNAPSHOT`，与新的 tag 对齐；
- 版本变更需要同步修改：
  - `spark-pudding-benchmark/VERSION`
  - README 和 RELEASE 文档中的版本号
  - `config/env/` 和 `scripts/` 中的发行包路径与 runtime jar 版本
  - `docs/` 中的版本示例；
- 本项目版本只跟随 `spark-pudding`，不与其他工程联动。

相关文档：

- [测试方案](docs/test-plan.md)
- [基准测试方案](docs/benchmark-plan.zh-CN.md)
- [测试发布记录](RELEASE.md)
- [缺陷模板](docs/bug-report-template.md)
- [本地开发流程](../spark-pudding-development-workflow.md)

## 一键运行

完整跑一遍 `1.0.0` 基准（预检、smoke、scale 数据准备、规模门禁、报告）：

```bash
scripts/run/bench-suite.sh all
```

首次运行会自动生成 `data/seed`（固定 seed，可重复）。也可以分阶段执行：

```bash
scripts/run/bench-suite.sh preflight   # 环境、版本、磁盘检查
scripts/run/bench-suite.sh smoke       # P0 smoke
scripts/run/bench-suite.sh prepare     # 生成并加载 scale 数据
scripts/run/bench-suite.sh scale       # E2E-01/04/06/09/14 规模门禁
scripts/run/bench-suite.sh report      # 汇总最新一次 suite
scripts/run/bench-suite.sh cleanup     # 停止所有 Docker profile
```

常用参数：

```bash
scripts/run/bench-suite.sh all --dry-run          # 只打印命令
scripts/run/bench-suite.sh all --keep-services    # 结束后保留服务
scripts/run/bench-suite.sh prepare --force-data   # 强制重建 scale 数据
scripts/run/bench-suite.sh scale --run-id <id>    # 复用指定 run id
```

报告输出：

```text
results/bench/suite-<run-id>/report.md
results/bench/benchmark-report.md       # 最新报告
```

完整运行需要 Docker Desktop 有足够内存、约 30 GB 磁盘，并按 profile 串行执行；耗时取决于
主机，通常为小时级。下面的“快速开始”是手动分步版本，便于调试单个用例。

## 项目定位

这个项目不是生产压测平台，而是可重复执行的**回归测试床**：

- 验证 batch、sync、streaming 三种作业模式；
- 验证 source、transform、sink 的组合和依赖校验；
- 验证 JDBC/MySQL/PostgreSQL/MongoDB CDC、Kafka、Schema Registry 和湖仓写入；
- 验证增量状态、锁、fencing token、checkpoint 重启和故障恢复；
- 验证对象存储、表维护、能力清单和负面错误路径；
- 在单机 Docker 环境采集可比较的 wall time、driver RSS、spill 和输入吞吐基线。

它不承诺：

- 多节点、生产数据量或真实 SLA；
- 与线上集群一致的资源隔离和网络条件；
- 长期 soak 测试；
- 在 GitHub Actions 中自动执行完整基准。

## 与 spark-pudding 的关系

默认目录结构：

```text
own-code/
├── spark-pudding/              被测项目
└── spark-pudding-benchmark/    本测试床
```

本项目的 job、Docker 服务和数据生成脚本不写入 `spark-pudding` 仓库。测试默认使用
`spark-pudding/build/target/spark-pudding-<version>/` 发行包，也可以通过
`PUDDING_HOME` 指向已解压的发布包。

## 模块说明

| 路径 | 功能 |
| --- | --- |
| `config/docker/` | Docker Compose、PostgreSQL/MySQL/MongoDB/Kafka/ES/MinIO/Redis/etcd/HDFS 服务、初始化 SQL |
| `config/env/` | `env.properties` 和 JDBC/Redis/etcd state 后端模板 |
| `config/jobs/` | 93 个 job 模板，覆盖 E2E、OS、SR、SK、RG、NG 用例；由 `scripts/run/generate_jobs.py` 生成 |
| `config/schemas/` | Avro、Protobuf schema |
| `data/seed/` | 确定性 smoke 数据；大文件由脚本生成，仅提交小型 e2e fixture |
| `data/generated/` | smoke/scale 生成数据；scale 三张主表各 ≥ 1 GiB，可重建，不提交 |
| `scripts/data-gen/` | 数据生成、Postgres/MySQL/Mongo/Kafka/ES/湖仓加载、规模校验 |
| `scripts/services/` | Compose profile 启停、健康检查、数据重置 |
| `scripts/run/` | 单用例、E2E、规模、恢复、负面、对象存储、维护和基准执行器；`bench-suite.sh` 总编排 |
| `scripts/verify/` | 表行数、checkpoint、增量状态和结果断言 |
| `results/data-scale/` | 数据集规模报告 |
| `results/runs/` | 每个用例的执行产物（日志、断言、摘要） |
| `results/bench/` | 基准指标、Spark event 日志、RSS/Docker 采样和基线报告 |
| `results/state/` | 湖仓、checkpoint、state 后端的运行状态；可删除，不提交 |
| `docs/` | 测试方案、基准方案、缺陷模板 |
| `tmp/` | 临时文件 |

## 快速开始

> 完整基准推荐直接使用 `scripts/run/bench-suite.sh all`。下面的步骤用于手动调试单个
> 用例或单个阶段；`bench-suite.sh` 会自动完成 profile 启停、数据准备和报告。

### 1. 前置条件

- JDK 11（推荐）或 JDK 17；
- Maven 3.8+；
- Docker Desktop，建议至少 8 GiB 内存、30 GiB 可用磁盘；
- Python 3.10+；
- Spark 3.4.0；
- `spark-pudding` 发行包。

### 2. 构建被测项目

```bash
cd ../spark-pudding
mvn -B clean verify
```

产物：

```text
spark-pudding/build/target/spark-pudding-1.0.0/
spark-pudding/build/target/spark-pudding-1.0.0.tar.gz
```

### 3. 准备环境

```bash
cd ../spark-pudding-benchmark

# Docker 服务配置；首次启动时 up.sh 也会自动创建
cp config/docker/.env.example config/docker/.env

# Spark/Pudding 路径
cp config/env/env.example.properties config/env/env.local.properties
chmod 600 config/env/env.local.properties
```

编辑 `config/env/env.local.properties`：

```properties
JAVA_HOME=/path/to/jdk-11
SPARK_HOME=/path/to/spark-3.4.0-bin-hadoop3
PUDDING_HOME=/path/to/spark-pudding-1.0.0
```

在当前 shell 中加载：

```bash
set -a
source config/env/env.local.properties
set +a
```

### 4. 启动服务

一次启动一个 profile，避免 Docker Desktop 内存不足：

```bash
scripts/services/up.sh relational    # PostgreSQL、MySQL、MongoDB
scripts/services/up.sh streaming     # Kafka、Schema Registry
scripts/services/up.sh search        # Elasticsearch
scripts/services/up.sh infra         # MinIO、Redis、etcd
scripts/services/up.sh hdfs          # NameNode、DataNode

scripts/services/status.sh
```

切换 profile：

```bash
scripts/services/down.sh relational
scripts/services/up.sh streaming
```

### 5. 加载 smoke 数据

`data/seed` 的大文件不进入 Git；首次使用时生成（固定 seed 42，100,000 行）：

```bash
python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/seed --formats csv,jsonl --force
```

然后加载：

```bash
scripts/services/up.sh relational

PUDDING_DATA_DIR="$PWD/data/seed" PUDDING_DATA_CONTAINER_DIR=/data-seed \
  scripts/data-gen/03_load_postgres.sh
PUDDING_DATA_DIR="$PWD/data/seed" PUDDING_DATA_CONTAINER_DIR=/data-seed \
  scripts/data-gen/04_load_mysql.sh

scripts/data-gen/setup-python.sh
.venv/bin/python scripts/data-gen/05_load_mongo.py \
  --data-dir data/seed --tables orders,customers,payments
```

`/data-seed` 是 compose 中 `data/seed` 的只读挂载；`data/generated` 仍挂载在 `/data`。

### 6. 跑一个用例

```bash
# 单用例
scripts/run/run-case.sh E2E-01 --dataset smoke

# file/Hive/transform smoke，不需要 lakehouse 数据
scripts/run/run-smoke-suite.sh file-basic smoke

# 一键 smoke：自动加载 seed、启停 relational/streaming/search
scripts/run/bench-suite.sh smoke
```

`run-case.sh` 负责渲染 `config/jobs/*.yaml`、替换数据路径、通过 client 提交作业。
需要哪个 Docker profile 由用例决定，见脚本头部说明。

### 7. 生成 scale 数据并跑规模用例

推荐使用编排入口：

```bash
scripts/run/bench-suite.sh prepare   # 生成/校验 scale 数据，加载 PG/MySQL，准备 Iceberg 表
scripts/run/bench-suite.sh scale     # E2E-01/04/06/09/14 规模门禁
scripts/run/bench-suite.sh report    # 生成统一报告
```

手动运行单个基准用例：

```bash
python3 scripts/data-gen/01_generate_tables.py \
  --profile scale --out data/generated --formats csv,jsonl --force

scripts/run/bench-case.sh E2E-04-scale --rows 10000002 \
  --result-case E2E-04-scale -- scripts/run/run-e2e.sh E2E-04-scale

scripts/run/bench-summary.py results/bench
```

`bench-case.sh` 会采集 wall time、Spark event 指标、driver RSS 和 Docker 资源采样，
输出到 `results/bench/<case>-<timestamp>/`；`bench-suite.sh report` 会写入
`results/bench/suite-<run-id>/report.md` 和 `results/bench/benchmark-report.md`。

### 8. 停止服务

```bash
scripts/run/bench-suite.sh cleanup
```

也可以按 profile 停止：

```bash
scripts/services/down.sh relational
scripts/services/down.sh streaming
scripts/services/down.sh search
scripts/services/down.sh infra
scripts/services/down.sh hdfs
```

## 测试点

### 功能维度

| 维度 | 覆盖 |
| --- | --- |
| 作业模式 | batch、sync full/incremental、streaming |
| Source | file、Hive、JDBC、MySQL/PostgreSQL/MongoDB CDC、Kafka、Elasticsearch、湖仓 |
| Transform | filter、column、aggregate、join、union、dedup、limit、validate、schema contract |
| Sink | file、Hive、JDBC、MySQL、Hudi、Paimon、Iceberg、Kafka、MongoDB、Elasticsearch |
| 增量语义 | append、upsert、CDC、cursor、fencing token、锁和事务恢复 |
| 流式能力 | checkpoint 重启、watermark、窗口聚合、stream-stream join、state TTL |
| 对象存储 | S3A checkpoint/state、Iceberg S3FileIO、HDFS、凭据隔离 |
| 运维 | checkpoint CLI、state CLI、capabilities、表维护、maintenance-server |
| 错误路径 | 多 sink、依赖环、缺 key、缺 schema、缺 secret、非法 CDC op、凭据不泄露 |
| 规模 | 1.3 GiB transform、10,000,000 行 JDBC、10M CDC、10M Kafka |

### 用例分层

| 层 | 内容 |
| --- | --- |
| P0 smoke | 组件和能力的最小可用路径，30/30 PASS |
| E2E | E2E-01 到 E2E-15，覆盖 transform、CDC、Kafka、湖仓、全量替换、round trip |
| SR/SK | source/sink 组合矩阵 |
| OS | 对象存储、checkpoint/state、HDFS、表维护、maintenance-server、capabilities |
| RG | kill -9、并发锁、pending transaction、Kafka/MySQL 重启、checkpoint 清理 |
| NG | 多 sink、未知依赖、依赖环、非法模式、缺失字段/secret、非法 CDC op |
| Scale | E2E-01/04/06/09/14 规模门禁 |

### 通过标准

- 作业退出码为 0，或按用例定义的有效完成条件满足；
- 目标表行数、内容、checkpoint/state 断言通过；
- checkpoint 重启后无重复；
- 错误信息包含出错的键，不包含凭据值；
- 规模用例记录 distribution hash、Spark event 日志和资源采样。

## 当前基准结果

单节点 macOS、JDK 11、Spark 3.4.0 `local[2]`、Docker Desktop 同机运行。详细环境、
过程和限制见 [RELEASE.md](RELEASE.md)。

| 用例 | 行数 | Wall | 有效完成 | Peak RSS | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| E2E-01 1.3 GiB transform | 10 输出行 | 23 s | 23 s | 1.05 GiB | PASS |
| E2E-14 Iceberg 1.3 GiB full refresh | 10,000,000 | 25 s | 25 s | 1.55 GiB | PASS |
| E2E-14 Hudi 1.3 GiB full refresh | 10,000,000 | 244 s | 244 s | 4.69 GiB | PASS |
| E2E-14 Paimon 1.3 GiB full refresh | 10,000,000 | 27 s | 27 s | 1.48 GiB | PASS |
| E2E-04 10M PostgreSQL incremental | 10,000,002 | 163 s | 146.3 s + 6.8 s | 4.77 GiB | PASS |
| E2E-06 10M MySQL CDC → Hudi | 10,000,013 | 4855 s | 3037 s | 8.68 GiB | PASS |
| E2E-06 10M MySQL CDC → Iceberg | 10,000,013 | 1021 s | 1021 s | 2.46 GiB | PASS |
| E2E-09 10M Kafka → Hudi | 10,000,000 | 1001 s | 195 s | 2.52 GiB | PASS |

这些数字是本机回归基线，不是生产 SLA。比较时要求同一主机、Spark 内存、Docker
profile 和数据 profile；wall time 回退超过 20% 或峰值 RSS 回退超过 25% 需要排查。

## 结果产物

```text
results/
├── data-scale/         数据规模报告
├── runs/               每个用例的日志、断言和摘要
├── bench/              基准 metrics、event 日志、RSS/Docker 采样
│   ├── README.md
│   ├── baseline-2026-11-11.md
│   └── <case>-<timestamp>/
└── state/              湖仓、checkpoint、state 后端运行状态
```

`data/generated/`、`results/runs/`、`results/bench/<case>-*/`、`results/state/` 和
`tmp/` 都是可删除的运行产物，不进入 Git。

## 文档索引

- [docs/test-plan.md](docs/test-plan.md)：完整测试方案、用例编号和断言
- [docs/benchmark-plan.zh-CN.md](docs/benchmark-plan.zh-CN.md)：基准方法、规模门禁和验收标准
- [RELEASE.md](RELEASE.md)：v1.0.0 测试过程、结果、缺陷和限制
- [docs/bug-report-template.md](docs/bug-report-template.md)：缺陷记录模板
- [scripts/run/README.zh-CN.md](scripts/run/README.zh-CN.md)：用例执行器参数
- [scripts/data-gen/README.zh-CN.md](scripts/data-gen/README.zh-CN.md)：数据生成和加载
- [config/docker/README.zh-CN.md](config/docker/README.zh-CN.md)：Docker 服务和端口
- [results/bench/README.zh-CN.md](results/bench/README.zh-CN.md)：基准产物格式

## 已知限制

- 单机 Docker 环境，Docker Desktop 内存有限，一次只跑一个重 profile；
- E2E-06 Hudi 10M CDC 是长作业，默认 runner 超时 80 分钟；
- Hudi/Paimon 10M full refresh 会新建表；Iceberg scale 用例需要预先建表；
- PostgreSQL 可能因 inactive replication slot 保留 WAL，10M 写入前需要清理；
- MinIO 测试镜像从 `quay.io/minio/minio` 拉取，Docker Hub 上的 `minio/minio`
  已不可用；
- 基准结果仅代表当前主机和当前发行包，不作为生产容量规划依据。
