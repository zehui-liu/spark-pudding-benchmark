# Benchmark results

English | [简体中文](README.zh-CN.md)

Each directory is one `bench-case.sh` run:

- `meta.env` — case, command, wall time, exit code, git HEAD/dirty hash and distribution;
- `jars.sha256` — distribution jar hashes;
- `events/` — Spark event logs (one subdirectory per `run-case.sh` invocation);
- `rss.csv` / `docker.csv` — resource samples;
- `results/<case>/` — the normal `results/runs/<case>` artifacts;
- `metrics.json` — parsed wall time, task/SQL duration, spill, shuffle, input and peak RSS;
- `metrics.stdout` — the parser output.

Render a table:

```shell
python3 scripts/run/bench-summary.py results/bench
```

Run a case:

```shell
./scripts/run/bench-case.sh E2E-01 --rows 10 --result-case E2E-01 -- \
  ./scripts/run/run-e2e.sh E2E-01
```

The benchmark harness does not change job templates. When
`PUDDING_BENCH_EVENT_ROOT` is set, `run-case.sh` adds `spark.eventLog.*` to the
rendered job only.

Suite runs are written under `suite-<run-id>/` with a `report.md`; the latest
suite report is also copied to `benchmark-report.md`. See
[`scripts/run/bench-suite.sh`](../../scripts/run/bench-suite.sh).

These are local single-node numbers (`local[2]`, Docker on a laptop). Use them
for regression comparisons on the same host; they are not production SLAs.
