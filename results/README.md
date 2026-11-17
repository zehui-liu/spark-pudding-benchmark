# results

[English](README.en.md) | 简体中文

测试结果目录：

- `data-scale/`：数据集规模校验报告（`size-report.json` 等）。
- `runs/<case-id>/`：每个用例的 `job.yaml`、`env.properties`、`submit.log`、
  `assert.log`、`result.json`，失败时保留最小复现。

约定：

- 结果目录不提交仓库；
- 每次运行使用时间戳或运行 ID 区分，避免覆盖历史；
- 缺陷证据直接引用对应结果目录。
