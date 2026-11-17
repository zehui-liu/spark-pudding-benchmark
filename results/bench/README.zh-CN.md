# 基准结果

[English](README.md) | 简体中文

每个目录对应一次 `bench-case.sh` 运行：

- `meta.env` — 用例、命令、wall time、退出码、git HEAD/dirty hash 和发行包；
- `jars.sha256` — 发行包 jar hash；
- `events/` — Spark event 日志（每次 `run-case.sh` 调用一个子目录）；
- `rss.csv` / `docker.csv` — 资源采样；
- `results/<case>/` — 普通的 `results/runs/<case>` 产物；
- `metrics.json` — 解析后的 wall time、task/SQL 时长、spill、shuffle、输入和峰值 RSS；
- `metrics.stdout` — 解析器输出。

渲染表格：

```shell
python3 scripts/run/bench-summary.py results/bench
```

运行一个用例：

```shell
./scripts/run/bench-case.sh E2E-01 --rows 10 --result-case E2E-01 -- \
  ./scripts/run/run-e2e.sh E2E-01
```

基准 harness 不修改 job 模板。设置 `PUDDING_BENCH_EVENT_ROOT` 时，`run-case.sh` 只会在
渲染后的 job 中加入 `spark.eventLog.*`。

套件运行写入 `suite-<run-id>/`，并生成 `report.md`；最新报告也会复制到
`benchmark-report.md`。见 [`scripts/run/bench-suite.sh`](../../scripts/run/bench-suite.sh)。

这些是本地单节点数字（`local[2]`、笔记本上的 Docker）。仅用于同一主机上的回归比较，
不是生产 SLA。
