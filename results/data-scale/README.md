# Scale data gate

English | [简体中文](README.zh-CN.md)

Per-profile reports are collected while each Docker profile is running, then
merged into `size-report.json`:

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

The gate requires at least 1 GiB of logical data for `orders`, `customers` and
`payments`. Paimon writes compressed Parquet by default (1.3.2 rejects
`file.compression=none`); its report records the uncompressed CSV logical size
and the compressed physical size separately.

Final status: `size-report.json` has `passed=true`, no `failures` and no
`missing` entries. Measured logical GiB:

| System | orders | customers | payments |
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
