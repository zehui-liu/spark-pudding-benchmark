#!/usr/bin/env python3
"""Build Hudi, Paimon or Iceberg baseline tables from the generated CSV files.

Run through 08_build_lakehouse.sh so the matching runtime jar and Spark settings
are supplied. Path tables are used so the dataset does not depend on a Hive
metastore.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA = ROOT / "data" / "generated"
DEFAULT_WAREHOUSE = DEFAULT_DATA / "lakehouse"

SCHEMAS = {
    "orders": StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("customer_id", LongType(), False),
            StructField("amount", DoubleType(), False),
            StructField("status", StringType(), False),
            StructField("city", StringType(), False),
            StructField("order_time", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("payload", StringType(), False),
        ]
    ),
    "customers": StructType(
        [
            StructField("customer_id", LongType(), False),
            StructField("name", StringType(), False),
            StructField("city", StringType(), False),
            StructField("segment", StringType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("payload", StringType(), False),
        ]
    ),
    "payments": StructType(
        [
            StructField("payment_id", LongType(), False),
            StructField("order_id", LongType(), False),
            StructField("paid_amount", DoubleType(), False),
            StructField("payment_time", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("payload", StringType(), False),
        ]
    ),
}

PRIMARY_KEYS = {
    "orders": "order_id",
    "customers": "customer_id",
    "payments": "payment_id",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("hudi", "paimon", "iceberg"), required=True)
    parser.add_argument("--tables", default="orders,customers,payments")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


SPARK_TYPES = {
    LongType: "BIGINT",
    DoubleType: "DOUBLE",
    StringType: "STRING",
    TimestampType: "TIMESTAMP",
}


def delete_path(spark: SparkSession, path: Path) -> None:
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path.resolve().as_uri())
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(hadoop_path):
        fs.delete(hadoop_path, True)


def paimon_ddl(table: str) -> str:
    columns = ",\n  ".join(
        f"{field.name} {SPARK_TYPES[type(field.dataType)]}"
        for field in SCHEMAS[table].fields
    )
    return (
        f"CREATE TABLE IF NOT EXISTS paimon.pudding_test.{table} (\n  {columns}\n) "
        "USING paimon TBLPROPERTIES ("
        f"'primary-key' = '{PRIMARY_KEYS[table]}', 'bucket' = '4')"
    )


def write_paimon(spark: SparkSession, table: str, frame) -> int:
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.pudding_test")
    spark.sql(paimon_ddl(table))
    view = f"src_{table}"
    frame.createOrReplaceTempView(view)
    spark.sql(f"INSERT INTO paimon.pudding_test.{table} SELECT * FROM {view}")
    return spark.table(f"paimon.pudding_test.{table}").count()


def iceberg_ddl(table: str) -> str:
    columns = ",\n  ".join(
        f"{field.name} {SPARK_TYPES[type(field.dataType)]}"
        for field in SCHEMAS[table].fields
    )
    return (
        f"CREATE TABLE IF NOT EXISTS iceberg.pudding_test.{table} (\n  {columns}\n) "
        "USING iceberg TBLPROPERTIES "
        "('write.parquet.compression-codec' = 'uncompressed')"
    )


def write_iceberg(spark: SparkSession, table: str, frame) -> int:
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.pudding_test")
    spark.sql(iceberg_ddl(table))
    view = f"src_{table}"
    frame.createOrReplaceTempView(view)
    spark.sql(f"INSERT INTO iceberg.pudding_test.{table} SELECT * FROM {view}")
    return spark.table(f"iceberg.pudding_test.{table}").count()


def write_table(spark: SparkSession, format_name: str, table: str, source: Path, target: Path, partitions: int, limit: int | None) -> int:
    if not source.exists():
        raise SystemExit(f"missing dataset: {source}")
    frame = (
        spark.read.option("header", True)
        .schema(SCHEMAS[table])
        .csv(str(source))
    )
    if limit is not None:
        frame = frame.limit(limit)
    frame = frame.repartition(partitions)

    if format_name == "paimon":
        count = write_paimon(spark, table, frame)
        print(f"[paimon/{table}] rows={count:,} table=paimon.pudding_test.{table}", flush=True)
        return count

    if format_name == "iceberg":
        count = write_iceberg(spark, table, frame)
        print(f"[iceberg/{table}] rows={count:,} table=iceberg.pudding_test.{table}", flush=True)
        return count

    delete_path(spark, target)
    target.parent.mkdir(parents=True, exist_ok=True)

    if format_name == "hudi":
        writer = (
            frame.write.format("hudi")
            .option("hoodie.table.name", table)
            .option("hoodie.datasource.write.recordkey.field", PRIMARY_KEYS[table])
            .option("hoodie.datasource.write.precombine.field", "updated_at")
            .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
            .option("hoodie.datasource.write.operation", "bulk_insert")
            .option("hoodie.datasource.write.partitionpath.field", "")
            .option(
                "hoodie.datasource.write.keygenerator.class",
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
            )
            .option("hoodie.datasource.hive_sync.enable", "false")
            .option("hoodie.metadata.enable", "false")
            .option("hoodie.parquet.compression.codec", "uncompressed")
        )
    else:
        writer = frame.write.format(format_name)

    writer.mode("append").save(str(target))
    count = spark.read.format(format_name).load(str(target)).count()
    print(f"[{format_name}/{table}] rows={count:,} path={target}", flush=True)
    return count


def main() -> int:
    args = parse_args()
    warehouse = args.warehouse.resolve()
    builder = (
        SparkSession.builder.appName(f"spark-pudding-benchmark-{args.format}-baseline")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
    )
    if args.format == "paimon":
        builder = (
            builder.config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", str(warehouse / "paimon"))
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
            )
        )
    elif args.format == "iceberg":
        builder = (
            builder.config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", str(warehouse / "iceberg"))
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    tables = [item.strip() for item in args.tables.split(",") if item.strip()]
    try:
        for table in tables:
            write_table(
                spark,
                args.format,
                table,
                args.data_dir / f"{table}.csv",
                warehouse / args.format / table,
                args.partitions,
                args.limit,
            )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
