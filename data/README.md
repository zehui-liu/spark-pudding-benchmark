# data

[English](README.en.md) | 简体中文

测试数据目录：

- `seed/`：确定性 smoke 数据（默认 10 万行）。`orders`、`customers`、`payments`
  的 CSV/JSONL 大文件不提交，首次使用时用
  `01_generate_tables.py --profile smoke --out data/seed --force` 生成；
  `e2e03`、`e2e11`、`e2e12` 等小型 fixture 仍然提交。
- `generated/`：1 GiB 规模数据（scale 档），由 `scripts/data-gen/` 生成，可随时重建，
  不提交仓库。

约定：

- 所有生成使用固定随机种子，保证可重复；
- 三张主测试表 `orders`、`customers`、`payments` 各自未压缩逻辑数据量 ≥ 1 GiB；
- 生成数据保留每张表的 CSV/JSONL 及 `sha256` 清单；
- 增量数据单独存放：`delta_insert.csv`、`delta_update.csv`、`delta_delete.csv`；
- 大文件只保留一份，各系统通过加载脚本导入，不复制多份原始文件。
