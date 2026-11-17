# Case runner

English | [简体中文](README.zh-CN.md)

`run-case.sh` renders one `config/jobs/<case>.yaml` and submits it through the
bundled client.

```shell
scripts/run/run-case.sh SR-01
scripts/run/run-case.sh SK-03 --dataset smoke --timeout 600
scripts/run/run-case.sh TR-01 --dry-run
```

Each run creates:

```text
results/runs/<case-id>/
  job.yaml          rendered job
  env.properties    submission environment
  submit.log        client and driver output
  output/           file-sink output, when the case writes one
  result.json       exit code, output row count and timestamps
```

A zero exit code alone is not treated as success for source/transform cases:
the runner requires at least one output JSON row and otherwise fails the case.
Sink target assertions are still done by the verifier scripts for the relevant
service.

## Suite orchestrator

`bench-suite.sh` runs the full 1.0.0 benchmark or individual phases:

```shell
scripts/run/bench-suite.sh all
scripts/run/bench-suite.sh preflight
scripts/run/bench-suite.sh smoke
scripts/run/bench-suite.sh prepare
scripts/run/bench-suite.sh scale
scripts/run/bench-suite.sh report
scripts/run/bench-suite.sh cleanup
```

`all` writes `results/bench/suite-<run-id>/report.md` and updates
`results/bench/benchmark-report.md`. Use `--dry-run`, `--keep-services`,
`--force-data` and `--run-id <id>` to control a run.

## Smoke suites

Services are managed separately so a small Docker VM only runs one profile:

```shell
# file / Hive / lakehouse / transforms: no Docker profile
scripts/run/run-smoke-suite.sh file smoke

# file / Hive / transforms only: no lakehouse data required
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

Environment knobs:

| Variable | Default |
| --- | --- |
| `PUDDING_HOME` | sibling `spark-pudding/build/target/spark-pudding-1.0.0` |
| `SPARK_HOME` | `/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3` |
| `PUDDING_JAVA_HOME` | arm64 Zulu 11 under `/Library/Java/JavaVirtualMachines` |
| `PUDDING_TEST_DATA` | set by `--dataset smoke|scale` |
| `PUDDING_BENCH_OUT_ROOT` | `results/bench`; suite runs use `results/bench/suite-<run-id>/bench` |

The current P0 smoke result is summarized in
[`results/runs/p0-smoke-summary.md`](../../results/runs/p0-smoke-summary.md).
