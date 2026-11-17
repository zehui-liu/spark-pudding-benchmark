#!/usr/bin/env python3
"""Create an empty Hudi copy-on-write table at a filesystem path.

Streaming Hudi upsert/CDC requires an existing table. A truly empty bulk insert
fails in Hudi's metadata update ("Positive number of partitions required"), so
this writes one bootstrap row and then deletes it by key. The resulting table
has zero live rows and can be used by the streaming sink.

Usage: spark-submit --jars hudi-runtime.jar create-hudi-table.py \
         <path> <table-name> [orders|cdc|hudi]

`orders` (default) matches the E2E-09 Kafka projection. `cdc` matches the full
orders schema used by the relational CDC cases.
"""

from __future__ import annotations

import sys
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def main() -> None:
    path, table = sys.argv[1:3]
    variant = sys.argv[3] if len(sys.argv) > 3 else "orders"
    spark = (
        SparkSession.builder.appName("pudding-create-hudi-table")
        .config("spark.ui.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    def options(operation: str) -> dict:
        return {
            "hoodie.table.name": table,
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": operation,
            "hoodie.datasource.write.recordkey.field": "order_id",
            "hoodie.datasource.write.precombine.field": "updated_at",
            "hoodie.datasource.write.partitionpath.field": "",
            "hoodie.datasource.write.keygenerator.class":
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        }

    try:
        if variant == "cdc":
            schema = StructType(
                [
                    StructField("order_id", LongType()),
                    StructField("customer_id", LongType()),
                    StructField("amount", DecimalType(12, 2)),
                    StructField("status", StringType()),
                    StructField("city", StringType()),
                    StructField("order_time", TimestampType()),
                    StructField("updated_at", TimestampType()),
                    StructField("payload", StringType()),
                ]
            )
            bootstrap = spark.createDataFrame(
                [(0, 0, Decimal("0.00"), "BOOT", "bootstrap", None, None, "bootstrap")],
                schema)
        elif variant == "hudi":
            schema = StructType(
                [
                    StructField("order_id", LongType()),
                    StructField("customer_id", LongType()),
                    StructField("amount", DoubleType()),
                    StructField("status", StringType()),
                    StructField("city", StringType()),
                    StructField("order_time", TimestampType()),
                    StructField("updated_at", TimestampType()),
                    StructField("payload", StringType()),
                ]
            )
            bootstrap = spark.createDataFrame(
                [(0, 0, 0.0, "BOOT", "bootstrap", None, None, "bootstrap")],
                schema)
        elif variant == "orders":
            schema = StructType(
                [
                    StructField("order_id", LongType()),
                    StructField("amount", DoubleType()),
                    StructField("city", StringType()),
                    StructField("updated_at", TimestampType()),
                    StructField("payload", StringType()),
                ]
            )
            bootstrap = spark.createDataFrame(
                [(0, -1.0, "bootstrap", None, "bootstrap")], schema)
        else:
            raise SystemExit(f"unknown schema variant: {variant}")

        (bootstrap.repartition(1).write.format("hudi")
         .options(**options("bulk_insert"))
         .mode("overwrite")
         .save(path))

        key = spark.createDataFrame(
            [(0,)], StructType([StructField("order_id", LongType())]))
        (key.repartition(1).write.format("hudi")
         .options(**options("delete"))
         .mode("append")
         .save(path))
    finally:
        spark.stop()
    print(f"created hudi table {table} at {path} ({variant})")


if __name__ == "__main__":
    main()
