# scripts

English | [简体中文](README.md)

Test scripts, grouped by responsibility:

- `data-gen/`: smoke/scale data generation, system loading and size checks.
- `services/`: Docker Compose start/stop, health checks, initialization and data reset.
- `run/`: case runners that render jobs, call `bin/pudding submit` and collect logs;
  `bench-suite.sh` provides full and phased benchmark orchestration.
- `verify/`: data, checkpoint, incremental-state and log assertions.

Conventions:

- Every script uses `set -euo pipefail`;
- `--dataset smoke|scale` and `--case <id>` are supported;
- Every script is re-runnable and preserves the failure state;
- Scripts read only from `config/` and `data/` and write to `results/` and `tmp/`.
