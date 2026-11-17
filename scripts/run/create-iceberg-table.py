#!/usr/bin/env python3
"""Create an empty Iceberg table in a Hadoop catalog.

Streaming Iceberg CDC uses MERGE and therefore needs an existing target table.

Usage: spark-submit --jars iceberg-runtime.jar create-iceberg-table.py \
         <warehouse> <database> <table> [cdc]

The `cdc` schema matches the full relational orders table. The table path is
<warehouse>/<database>/<table>, matching the Hadoop catalog layout.
"""

from __future__ import annotations

import re
import sys

from pyspark.sql import SparkSession

SCHEMAS = {
    "cdc": """
        order_id    BIGINT,
        customer_id BIGINT,
        amount      DECIMAL(12, 2),
        status      STRING,
        city        STRING,
        order_time  TIMESTAMP,
        updated_at  TIMESTAMP,
        payload     STRING
    """,
}


def main() -> None:
    warehouse, database, table = sys.argv[1:4]
    variant = sys.argv[4] if len(sys.argv) > 4 else "cdc"
    if variant not in SCHEMAS:
        raise SystemExit(f"unknown schema variant: {variant}")
    for value in (database, table):
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise SystemExit(f"invalid Iceberg identifier: {value}")

    spark = (
        SparkSession.builder.appName("pudding-create-iceberg-table")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg_lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_lake.type", "hadoop")
        .config("spark.sql.catalog.iceberg_lake.warehouse", warehouse)
        .getOrCreate()
    )
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS iceberg_lake.{database}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS iceberg_lake.{database}.{table} (
                {SCHEMAS[variant]}
            ) USING iceberg
            TBLPROPERTIES ('format-version' = '2', 'write.upsert.enabled' = 'true')
            """
        )
    finally:
        spark.stop()
    print(f"created iceberg table iceberg_lake.{database}.{table} ({variant})")


if __name__ == "__main__":
    main()
