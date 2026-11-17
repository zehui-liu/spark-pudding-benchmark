# 用例执行器

[English](README.md) | 简体中文

`run-case.sh` 渲染一个 `config/jobs/<case>.yaml`，通过 bundled client 提交。

```shell
scripts/run/run-case.sh SR-01
scripts/run/run-case.sh SK-03 --dataset smoke --timeout 600
scripts/run/run-case.sh TR-01 --dry-run
```

每次运行创建：

```text
results/runs/<case-id>/
  job.yaml          渲染后的 job
  env.properties    提交环境
  submit.log        client 和 driver 输出
  output/           file sink 输出（用例写入文件时）
  result.json       退出码、输出行数和时间戳
```

对 source/transform 用例，退出码为 0 本身不算成功：runner 要求至少有一行输出 JSON，
否则判定失败。sink 目标端断言仍由对应服务的 verifier 脚本完成。

## 套件编排

`bench-suite.sh` 可以跑完整 1.0.0 基准，也可以按阶段执行：

```shell
scripts/run/bench-suite.sh all
scripts/run/bench-suite.sh preflight
scripts/run/bench-suite.sh smoke
scripts/run/bench-suite.sh prepare
scripts/run/bench-suite.sh scale
scripts/run/bench-suite.sh report
scripts/run/bench-suite.sh cleanup
```

`all` 输出 `results/bench/suite-<run-id>/report.md`，并更新
`results/bench/benchmark-report.md`。常用参数：`--dry-run`、`--keep-services`、
`--force-data`、`--run-id <id>`。

## Smoke 套件

服务由外部管理，使小型 Docker VM 一次只运行一个 profile：

```shell
# file / Hive / lakehouse / transforms：不需要 Docker profile
scripts/run/run-smoke-suite.sh file smoke

# 仅 file / Hive / transforms：不需要 lakehouse 数据
scripts/run/run-smoke-suite.sh file-basic smoke

# relational profile
scripts/services/up.sh relational
scripts/run/prepare-mongo-smoke.sh
scripts/run/run-smoke-suite.sh relational smoke
scripts/services/down.sh relational

# streaming profile
scripts/services/up.sh streaming
scripts/run/prepare-kafka-smoke.sh
scripts/run/run-smoke-suite.sh streaming smoke
scripts/services/down.sh streaming

# search profile
scripts/services/up.sh search
scripts/run/run-smoke-suite.sh search smoke
scripts/services/down.sh search
```

环境变量：

| 变量 | 默认值 |
| --- | --- |
| `PUDDING_HOME` | 同级 `spark-pudding/build/target/spark-pudding-1.0.0` |
| `SPARK_HOME` | `/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3` |
| `PUDDING_JAVA_HOME` | `/Library/Java/JavaVirtualMachines` 下的 arm64 Zulu 11 |
| `PUDDING_TEST_DATA` | 由 `--dataset smoke|scale` 设置 |
| `PUDDING_BENCH_OUT_ROOT` | `results/bench`；suite 使用 `results/bench/suite-<run-id>/bench` |

当前 P0 smoke 结果汇总在
[`results/runs/p0-smoke-summary.md`](../../results/runs/p0-smoke-summary.md)。
