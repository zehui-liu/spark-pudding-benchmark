# 规模数据门禁

[English](README.md) | 简体中文

每个 Docker profile 运行时收集对应报告，最后合并到 `size-report.json`：

```shell
python3 scripts/data-gen/09_verify_sizes.py --systems postgres,mysql,mongo \
  --out results/data-scale/size-relational.json
python3 scripts/data-gen/09_verify_sizes.py --systems kafka \
  --out results/data-scale/size-streaming.json
python3 scripts/data-gen/09_verify_sizes.py --systems es \
  --out results/data-scale/size-search.json
python3 scripts/data-gen/09_verify_sizes.py --systems lakehouse \
  --out results/data-scale/size-lakehouse.json
python3 scripts/data-gen/09_verify_sizes.py --systems csv \
  --out results/data-scale/size-csv.json

python3 scripts/data-gen/10_merge_size_reports.py \
  results/data-scale/size-*.json
```

门禁要求 `orders`、`customers`、`payments` 三张表各有至少 1 GiB 逻辑数据。Paimon 默认写
压缩 Parquet（1.3.2 拒绝 `file.compression=none`）；其报告分别记录未压缩 CSV 逻辑大小和
压缩后物理大小。

最终状态：`size-report.json` 的 `passed=true`，没有 `failures` 和 `missing` 条目。实测
逻辑 GiB：

| 系统 | orders | customers | payments |
| --- | --- | --- | --- |
| CSV | 1.321 | 1.450 | 1.197 |
| PostgreSQL | 1.871 | 2.142 | 1.830 |
| MySQL | 1.992 | 1.784 | 1.679 |
| MongoDB | 2.395 | 2.326 | 2.114 |
| Kafka | 2.791 | — | — |
| Elasticsearch | 1.617 | 1.237 | 1.463 |
| Hudi | 1.321 | 1.450 | 1.197 |
| Paimon | 1.321 | 1.450 | 1.197 |
| Iceberg | 1.321 | 1.450 | 1.197 |
