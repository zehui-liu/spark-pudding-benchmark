# 组件和 E2E job 模板

[English](README.md) | 简体中文

34 个组件模板（`SR-*`、`SK-*`、`TR-*`）由
[`scripts/run/generate_jobs.py`](../../scripts/run/generate_jobs.py) 生成。不要手工修改
生成文件；修改生成器后重新运行：

```shell
python3 scripts/run/generate_jobs.py
```

E2E 模板（`E2E-*`）需要多次运行和状态，因此为手写；例外是七个
`E2E-14-*-full-refresh` 变体，它们由
[`scripts/run/generate_e2e14_jobs.py`](../../scripts/run/generate_e2e14_jobs.py) 生成。

## 占位符

`scripts/run/run-case.sh` 会用 `envsubst` 按用例替换：

| 占位符 | 含义 |
| --- | --- |
| `${SPARK_HOME}` | Spark 安装目录 |
| `${PUDDING_HOME}` | 解压后的 spark-pudding 发行包 |
| `${PUDDING_TEST_ROOT}` | 本测试床根目录 |
| `${PUDDING_TEST_DATA}` | `data/seed`（smoke）或 `data/generated`（scale） |
| `${CASE_OUT}` | `results/runs/<case>/output` |
| `${PG_URL}`、`${PG_USER}`、`${PG_PASSWORD}` | PostgreSQL 源库 |
| `${PG_TARGET_URL}` | PostgreSQL 目标库（`target_db`） |
| `${MYSQL_URL}`、`${MYSQL_USER}`、`${MYSQL_PASSWORD}` | MySQL 源库 |
| `${MYSQL_TARGET_URL}` | MySQL 目标库（`target_db`） |
| `${MONGO_URI}` | MongoDB 副本集 URI |
| `${KAFKA_BOOTSTRAP}` | Kafka bootstrap servers |
| `${ES_PORT}` | Elasticsearch HTTP 端口 |

## 用例

| 范围 | 数量 | 说明 |
| --- | --- | --- |
| `SR-01` … `SR-13` | 13 种 source | `SR-11` … `SR-13` 是仅流式的 CDC 模板 |
| `SK-01` … `SK-11` | 11 种 sink | `SK-08` 必须在 `SR-07` 前运行以创建 Hive 表 |
| `TR-01` … `TR-10` | 10 种 transform | file source → transform → file sink |
| `E2E-01`、`E2E-04`、`E2E-05`、`E2E-09` | 4 条端到端路径 | 通过 `scripts/run/run-e2e.sh` 运行 |
| `E2E-14-*-full-refresh` | 7 种 sink | `sync full`；先用 `E2E14_LIMIT=100` 运行，再用 `200` 运行 |

前十个 source 模板以及所有 sink/transform 模板都是有限批处理作业。CDC 模板使用
processing-time trigger，由 runner 以 `--allow-timeout` 停止。

组件级 CDC 模板使用 file sink，使 runner 不必再启动 Spark 作业就能断言行内的
`__op`/`_cdc_sequence`。Paimon/Iceberg/Hudi 的 sink 行为由 `SK-*` 和 E2E 用例覆盖。

## 前置条件

- `SR-04` 和 `SR-13` 需要 10,000 文档的 `source_db.orders_smoke` 集合：
  `scripts/run/prepare-mongo-smoke.sh`。
- `SR-12`、`E2E-04` 需要 10,000 行的 PostgreSQL `source_db.orders_smoke` 表：
  `scripts/run/prepare-postgres-smoke.sh`。
- `E2E-05` 需要 10,000 行的 MySQL `source_db.orders_smoke` 表：
  `scripts/run/prepare-mysql-smoke.sh`。
- `SR-06` 和 `E2E-09` 需要 `orders-smoke` topic：
  `scripts/run/prepare-kafka-smoke.sh`。
- `E2E-05` / `E2E-09` 需要预先创建目标表，因为流式湖仓 CDC/upsert 要求表已存在：
  `scripts/run/create-paimon-table.sh` 和 `scripts/run/create-hudi-table.sh`
  （两者都由 `run-e2e.sh` 调用）。
- `SR-07` 需要 `SK-08` 创建 Hive 表。
- `SR-02`/`SR-03` 需要 PostgreSQL/MySQL JDBC driver 在 Spark classpath 上；测试床当前把
  `postgresql-42.7.3.jar` 和 `mysql-connector-java-8.0.30.jar` 安装到 `$SPARK_HOME/jars`。
- 湖仓 source 用例读取 `data/generated/lakehouse` 下的 1 GiB 基线表。

## E2E 状态

多次运行的用例把状态放在 `results/runs/` 之外，以便安全重跑：

| 用例 | 持久状态 | 目标 |
| --- | --- | --- |
| `E2E-04` | `results/state/e2e-04` | `target_db.e2e04_orders` |
| `E2E-05` | `results/state/e2e-05/checkpoint` | Paimon `pudding_test.db/e2e05_orders` |
| `E2E-09` | `results/state/e2e-09/{checkpoint,hudi/orders}` | Hudi `e2e09_orders` |
| `E2E-14` | `results/state/e2e-14` | file/Hive/JDBC/MySQL/Hudi/Paimon/Iceberg |

`run-e2e.sh` 在首次运行前重置这些路径和 smoke 源表。
