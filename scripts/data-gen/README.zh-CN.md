# 数据生成和加载

[English](README.md) | 简体中文

所有脚本都从自身位置解析测试床根目录，因此可以在任意目录运行：

```shell
cd /Users/zhliu/code/own-code/spark-pudding-benchmark
```

## 前置条件

- Docker Compose v2 和测试床镜像；
- arm64 JDK 11（或 17）、Spark 3.4.0；
- 已组装的 spark-pudding 发行包；
- Python 3.11 和加载器依赖：

```shell
scripts/data-gen/setup-python.sh
```

生成数据写入 `data/generated/`；smoke 数据约 200 MB，scale 数据每张主表至少 1 GiB。

## 生成

```shell
# 快速迭代：每张表 100,000 行
python3 scripts/data-gen/01_generate_tables.py \
  --profile smoke --out data/generated --formats csv,jsonl --force

# 规模门禁：每张表 10,000,000 行，每张表 >= 1 GiB
python3 scripts/data-gen/01_generate_tables.py \
  --profile scale --out data/generated --formats csv,jsonl --force

# 增量变更集，用于 sync/CDC 用例
python3 scripts/data-gen/02_generate_delta.py \
  --base-rows 10000000 --out data/generated --force
```

`manifest.json` 记录行数、文件大小、SHA-256 和 profile。scale 命令在任一输出小于 1 GiB
时打印警告。

## 加载关系型数据

先启动 profile：

```shell
scripts/services/up.sh relational
```

然后加载：

```shell
scripts/data-gen/03_load_postgres.sh
scripts/data-gen/04_load_mysql.sh
.venv/bin/python scripts/data-gen/05_load_mongo.py \
  --data-dir data/generated --tables orders,customers,payments
```

PostgreSQL 和 MySQL 脚本会先截断目标表，然后打印行数和表大小。`PUDDING_DATA_DIR`
选择宿主机数据目录；`PUDDING_DATA_CONTAINER_DIR` 选择对应的容器挂载路径
（`data/generated` 对应 `/data`，`data/seed` 对应 `/data-seed`）。

## 加载 Kafka

```shell
scripts/services/up.sh streaming

# JSON：不需要 Schema Registry
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/generated/orders.jsonl --format json

# Avro / Protobuf：在 Schema Registry 注册 subject
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/generated/orders.jsonl --format avro
.venv/bin/python scripts/data-gen/06_produce_kafka.py \
  --file data/generated/orders.jsonl --format protobuf
```

快速检查使用 `--limit 5000`。`--rate` 限制每秒消息数。

## 加载 Elasticsearch

```shell
scripts/services/up.sh search
.venv/bin/python scripts/data-gen/07_load_es.py \
  --data-dir data/generated --tables orders,customers,payments
```

对 10M 行 scale 数据，每个进程加载一张表能更充分地利用索引线程：

```shell
for table in orders customers payments; do
  .venv/bin/python scripts/data-gen/07_load_es.py \
    --data-dir data/generated --tables "$table" --batch-size 50000 \
    > "/tmp/pudding-es-$table.log" 2>&1 &
done
wait
```

脚本会重建索引并使用业务 id 作为 `_id`。

## 构建湖仓基线

包装脚本会添加对应的 runtime jar 和 Spark 配置：

```shell
scripts/data-gen/08_build_lakehouse.sh --format hudi
scripts/data-gen/08_build_lakehouse.sh --format paimon
scripts/data-gen/08_build_lakehouse.sh --format iceberg
```

快速运行使用 `--tables orders` 或 `--limit 1000`。Hudi 写入 path 表；Paimon 和 Iceberg
使用 `data/generated/lakehouse` 下的本地 catalog。

## 校验 1 GiB 门禁

Docker VM 较小，因此在每个 profile 运行时分别检查，最后合并报告：

```shell
# relational 正在运行
python3 scripts/data-gen/09_verify_sizes.py \
  --systems postgres,mysql,mongo \
  --out results/data-scale/size-relational.json

# streaming 正在运行
python3 scripts/data-gen/09_verify_sizes.py \
  --systems kafka \
  --out results/data-scale/size-streaming.json

# search 正在运行
python3 scripts/data-gen/09_verify_sizes.py \
  --systems es \
  --out results/data-scale/size-search.json

# lakehouse 文件在本地
python3 scripts/data-gen/09_verify_sizes.py \
  --systems lakehouse \
  --out results/data-scale/size-lakehouse.json

# 仅 CSV 的快速检查
python3 scripts/data-gen/09_verify_sizes.py \
  --systems csv --out results/data-scale/size-csv.json

# 合并为最终门禁报告
python3 scripts/data-gen/10_merge_size_reports.py \
  results/data-scale/size-*.json
```

加载后的表小于 1 GiB 时，verifier 以非零状态退出。合并脚本在请求的系统缺少报告时也会
失败。当前 1 GiB 运行已通过；见
[results/data-scale/README.zh-CN.md](../../results/data-scale/README.zh-CN.md)。

## 一次只跑一个 profile

compose 文件支持并发 profile，但当前 Docker VM 只有约 4 GiB 内存。推荐顺序：

1. `relational` → 加载 PostgreSQL/MySQL/MongoDB；
2. `streaming` → Kafka 和 Schema Registry；
3. `search` → Elasticsearch；
4. `infra` → MinIO/Redis/etcd；
5. `hdfs`（可选）。

启动下一个内存较重的 profile 前，用 `scripts/services/down.sh <profile>` 停止当前 profile。
`down.sh` 只停止该 profile，数据卷保留。
