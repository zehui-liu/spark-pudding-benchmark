#!/usr/bin/env python3
"""Create a Paimon database in a filesystem warehouse.

`saveAsTable` creates the table but not the namespace. Full-refresh cases with
an isolated warehouse need the database first.

Usage: spark-submit --jars paimon-runtime.jar create-paimon-database.py \
         <warehouse> <database>
"""

from __future__ import annotations

import re
import sys

from pyspark.sql import SparkSession


def main() -> None:
    warehouse, database = sys.argv[1:3]
    if not re.fullmatch(r"[A-Za-z0-9_]+", database):
        raise SystemExit(f"invalid Paimon database: {database}")

    spark = (
        SparkSession.builder.appName("pudding-create-paimon-database")
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
    finally:
        spark.stop()
    print(f"created paimon database paimon_lake.{database}")


if __name__ == "__main__":
    main()
