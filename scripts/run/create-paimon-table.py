#!/usr/bin/env python3
"""Create an empty Paimon primary-key table in a filesystem warehouse.

Streaming Paimon CDC requires the target table to exist before the query starts;
path-mode jobs therefore need this one-time bootstrap.

Usage: spark-submit --jars paimon-runtime.jar create-paimon-table.py \
         <warehouse> <database> <table> [orders|mongo|avro|hudi]

`orders` (default) uses order_id as the primary key and matches the relational
CDC cases. `mongo` adds the `_id` column and uses it as the primary key, because
MongoDB delete events only carry the document key.

The table path is <warehouse>/<database>.db/<table>, matching the path used by
the job.
"""

from __future__ import annotations

import re
import sys

from pyspark.sql import SparkSession

SCHEMAS = {
    "orders": (
        """
        order_id    BIGINT,
        customer_id BIGINT,
        amount      DECIMAL(12, 2),
        status      STRING,
        city        STRING,
        order_time  TIMESTAMP,
        updated_at  TIMESTAMP,
        payload     STRING
        """,
        "order_id",
    ),
    "mongo": (
        """
        _id         STRING,
        order_id    BIGINT,
        customer_id BIGINT,
        amount      DOUBLE,
        status      STRING,
        city        STRING,
        order_time  TIMESTAMP,
        updated_at  TIMESTAMP,
        payload     STRING
        """,
        "_id",
    ),
    "avro": (
        """
        order_id    BIGINT,
        customer_id BIGINT,
        amount      DOUBLE,
        status      STRING,
        city        STRING,
        order_time  STRING,
        updated_at  STRING,
        payload     STRING
        """,
        "order_id",
    ),
    "hudi": (
        """
        order_id    BIGINT,
        customer_id BIGINT,
        amount      DOUBLE,
        status      STRING,
        city        STRING,
        order_time  TIMESTAMP,
        updated_at  TIMESTAMP,
        payload     STRING
        """,
        "order_id",
    ),
}


def main() -> None:
    warehouse, database, table = sys.argv[1:4]
    variant = sys.argv[4] if len(sys.argv) > 4 else "orders"
    if variant not in SCHEMAS:
        raise SystemExit(f"unknown schema variant: {variant}")
    for value in (database, table):
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise SystemExit(f"invalid Paimon identifier: {value}")
    columns, primary_key = SCHEMAS[variant]

    spark = (
        SparkSession.builder.appName("pudding-create-paimon-table")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .config("spark.sql.catalog.paimon_lake", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon_lake.warehouse", warehouse)
        .getOrCreate()
    )
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS paimon_lake.{database}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS paimon_lake.{database}.{table} (
                {columns}
            ) USING paimon
            TBLPROPERTIES ('primary-key' = '{primary_key}', 'bucket' = '1')
            """
        )
    finally:
        spark.stop()
    print(f"created paimon table paimon_lake.{database}.{table} ({variant})")


if __name__ == "__main__":
    main()
