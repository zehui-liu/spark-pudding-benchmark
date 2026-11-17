# scripts

[English](README.en.md) | 简体中文

测试脚本，按职责分组：

- `data-gen/`：smoke/scale 数据生成、各系统加载、体积校验。
- `services/`：Docker Compose 启停、健康检查、初始化、数据重置。
- `run/`：用例执行器，负责渲染 job、调用 `bin/pudding submit`、收集日志；`bench-suite.sh`
  提供完整基准和分阶段编排。
- `verify/`：数据、checkpoint、增量状态和日志断言。

约定：

- 所有脚本使用 `set -euo pipefail`；
- 支持 `--dataset smoke|scale` 和 `--case <id>`；
- 每个脚本可重复执行，失败时保留现场；
- 脚本只读取 `config/` 和 `data/`，输出写入 `results/` 和 `tmp/`。
