# 基准测试方案（当前工作区）

[English](benchmark-plan.md) | 简体中文

本方案为 `spark-pudding v1.0.0` 建立本地单节点基线。它是回归基线，不是生产 SLA。

## 指纹

每次基准运行记录：

```text
git rev-parse HEAD
git status --porcelain | shasum -a 256
sha256 of build/target/spark-pudding-1.0.0/lib/*.jar
```

2026-11-11 的基线记录于 `v1.0.0` tag 最终确定之前。`results/bench/baseline-2026-11-11.md`
中的 commit hash 指向该预发布快照；tree hash 和发行包 jar hash 才是权威标识。

## 执行框架

- `scripts/run/bench-case.sh` 包装 `run-case.sh` / `run-e2e.sh`，采样 driver RSS 和 Docker
  统计，为每次调用开启 Spark event 日志，并把普通结果目录复制到
  `results/bench/<case>-<timestamp>/`。
- `scripts/run/parse-spark-events.py` 生成 `metrics.json`，包含 wall time、Spark application
  时间、SQL/job 时长、task 数、输入字节、spill、shuffle、driver RSS 峰值和 Docker 容器峰值。
- `scripts/run/bench-summary.py results/bench` 渲染对比表。
- `results/bench/README.md` 说明产物布局。

运行：

```shell
cd /Users/zhliu/code/own-code/spark-pudding-benchmark

./scripts/run/bench-case.sh E2E-01 --rows 10 --result-case E2E-01 -- \
  env E2E01_DATASET=scale scripts/run/run-e2e.sh E2E-01

./scripts/run/bench-case.sh E2E-14-iceberg-scale --rows 10000000 \
  --result-case E2E-14-iceberg-full-refresh-scale -- \
  scripts/run/run-case.sh E2E-14-iceberg-full-refresh-scale \
  --dataset scale --timeout 3600

./scripts/run/bench-case.sh E2E-04-scale --rows 10000002 \
  --result-case E2E-04-scale -- scripts/run/run-e2e.sh E2E-04-scale

./scripts/run/bench-case.sh E2E-06-hudi-scale --rows 10000013 \
  --result-case E2E-06-hudi-mysql-cdc-scale -- \
  scripts/run/run-e2e.sh E2E-06-scale

./scripts/run/bench-case.sh E2E-09-scale --rows 10000000 \
  --result-case E2E-09-kafka-to-hudi-scale -- \
  scripts/run/run-kafka-scale.sh
```

## P0 — 当前代码规模门禁

| 用例 | 数据 | 断言 |
| --- | --- | --- |
| E2E-01 | 1.3 GiB CSV | transform 链返回 10 行聚合结果 |
| E2E-14 Iceberg | 1.3 GiB CSV | 已有 200 行表被替换为 10,000,000 行 |
| E2E-04 | 10M PostgreSQL | 全量 + 2 新增 + 1 更新 = 10,000,002 目标行 |
| E2E-06 Hudi | 10,000,013 行 MySQL CDC 快照 | Hudi 精确行数 |
| E2E-09 | 10,000,000 条 Kafka JSON | Hudi 精确行数，checkpoint 重启无重复 |

## P1 — 审计修复引入的风险路径

- MySQL CDC 10M → Iceberg（`E2E-06-mysql-cdc-to-iceberg-scale`）。
- Hudi 1.3 GiB 全量替换（`E2E-14-hudi-full-refresh-scale`）。
- Paimon 1.3 GiB 全量替换（`E2E-14-paimon-full-refresh-scale`）。
- Hudi InternalRow/Jackson image 解码：bulk-insert/insert-only CDC，包括 binary 和嵌套值；
  DECIMAL 必须 fail-closed。
- Hudi archived timeline 在大 archive 下的扫描。
- Iceberg 多 snapshot 下的 sequence 查询；MERGE fencing 开销。
- JDBC metadata fallback 微基准（仅正确性）。

## 自动化

`scripts/run/bench-suite.sh` 自动执行可重复的 P0 路径：

```shell
scripts/run/bench-suite.sh all
```

依次执行预检、P0 smoke、scale 数据准备、E2E-01/04/06/09/14 规模门禁和报告。也可以
通过 `preflight`、`smoke`、`prepare`、`scale`、`report`、`cleanup` 分阶段执行。结果写入
`results/bench/suite-<run-id>/`，最新报告复制到 `results/bench/benchmark-report.md`。

## 验收

- 正确性断言必须通过；行数必须精确。
- 首次运行建立基线。后续比较使用同一主机、Spark 内存、Docker profile 和数据 profile。
- wall time 回退超过 20% 或峰值 RSS 回退超过 25% 需要调查。
- 不要混用 `smoke` 和 `scale` 结果目录。
- 每次运行都必须有 Spark event 日志和发行包 hash。

## 已知环境限制

- Docker Desktop 当前约 3.8 GiB 内存；一次只跑一个内存较重的 profile。
- PostgreSQL 可能因 inactive replication slot 保留 WAL。10M 写入前清理陈旧 slot；
  命中该问题的基准会以 `No space left on device` 失败。
- Hudi 10M MySQL 快照是长作业。E2E-06 runner 因此默认使用 80 分钟超时。
