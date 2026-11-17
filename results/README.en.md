# results

English | [简体中文](README.md)

Test result directories:

- `data-scale/`: data set size reports (`size-report.json` and related files).
- `runs/<case-id>/`: per-case `job.yaml`, `env.properties`, `submit.log`, `assert.log` and
  `result.json`; a minimal reproduction is retained on failure.

Conventions:

- Result directories are not committed;
- Each run uses a timestamp or run ID to avoid overwriting history;
- Bug evidence references the corresponding result directory.
