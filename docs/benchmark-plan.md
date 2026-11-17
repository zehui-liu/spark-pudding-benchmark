# Benchmark plan (current workspace)

English | [简体中文](benchmark-plan.zh-CN.md)

This plan establishes a local single-node baseline for `spark-pudding v1.0.0`.
It is a regression baseline, not a production SLA.

## Fingerprint

Every benchmark run records:

```text
git rev-parse HEAD
git status --porcelain | shasum -a 256
sha256 of build/target/spark-pudding-1.0.0/lib/*.jar
```

The 2026-11-11 baseline was recorded before the `v1.0.0` tag was finalized. The
commit hash in `results/bench/baseline-2026-11-11.md` refers to that pre-release
snapshot; the tree hash and distribution jar hashes are the authoritative
identifiers.

## Harness

- `scripts/run/bench-case.sh` wraps `run-case.sh` / `run-e2e.sh`, samples
  driver RSS and Docker stats, enables Spark event logging per invocation, and
  copies the normal result directory to `results/bench/<case>-<timestamp>/`.
- `scripts/run/parse-spark-events.py` writes `metrics.json` with wall time,
  Spark application time, SQL/job durations, task counts, input bytes, spill,
  shuffle, peak driver RSS and Docker container peaks.
- `scripts/run/bench-summary.py results/bench` renders the comparison table.
- `results/bench/README.md` documents the artifact layout.

Run:

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

## P0 — current-code scale gates

| Case | Data | Assertion |
| --- | --- | --- |
| E2E-01 | 1.3 GiB CSV | transform chain returns 10 aggregate rows |
| E2E-14 Iceberg | 1.3 GiB CSV | existing 200-row table replaced by 10,000,000 rows |
| E2E-04 | 10M PostgreSQL | full + 2 inserts + 1 update = 10,000,002 target rows |
| E2E-06 Hudi | 10,000,013 MySQL CDC snapshot | exact Hudi row count |
| E2E-09 | 10,000,000 Kafka JSON | exact Hudi row count, checkpoint restart clean |

## P1 — risk paths introduced by the audit fixes

- MySQL CDC 10M → Iceberg (`E2E-06-mysql-cdc-to-iceberg-scale`).
- Hudi 1.3 GiB full refresh (`E2E-14-hudi-full-refresh-scale`).
- Paimon 1.3 GiB full refresh (`E2E-14-paimon-full-refresh-scale`).
- Hudi InternalRow/Jackson image decode: bulk-insert/insert-only CDC, including
  binary and nested values; DECIMAL must fail closed.
- Hudi archived-timeline scan with a large archive.
- Iceberg sequence lookup with many snapshots; MERGE fencing overhead.
- JDBC metadata fallback microbenchmark (correctness only).

## Automation

`scripts/run/bench-suite.sh` automates the repeatable P0 path:

```shell
scripts/run/bench-suite.sh all
```

It runs preflight, P0 smoke, scale data preparation, the E2E-01/04/06/09/14 scale
gates and a report. Individual phases are available as `preflight`, `smoke`,
`prepare`, `scale`, `report` and `cleanup`. Results are written under
`results/bench/suite-<run-id>/` and the latest report is copied to
`results/bench/benchmark-report.md`.

## Acceptance

- Correctness assertions must pass; row counts must be exact.
- First run establishes the baseline. Later comparisons use the same host,
  Spark memory, Docker profile and data profile.
- A regression above 20% wall time or 25% peak RSS needs investigation.
- Do not mix `smoke` and `scale` result directories.
- Spark event logs and the distribution hash must be present for every run.

## Known environment caveats

- Docker Desktop currently has ~3.8 GiB memory; run one memory-heavy profile at
  a time.
- PostgreSQL can retain WAL through an inactive replication slot. Remove stale
  slots before a 10M write; the benchmark that hit this failed with
  `No space left on device`.
- Hudi's 10M MySQL snapshot is a long job. The E2E-06 runner uses an 80-minute
  timeout by default for this reason.
