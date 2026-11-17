#!/usr/bin/env python3
"""Assert the contents of a lakehouse table through Spark.

Usage:
  spark-submit --jars <runtime.jar> paimon-check.py <path> \
    --format paimon --count 10000 \
    --exists "order_id = 1000000001 AND amount = 999.99" \
    --missing "order_id = 1000000002"

The script exits non-zero with an ASSERT line on the first failed expectation.
Spark progress output is intentionally loud; callers grep for the ASSERT/PASS
lines in the captured log.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession


def fail(message: str) -> None:
    print(f"ASSERT FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--format", default="paimon")
    parser.add_argument("--count", type=int)
    parser.add_argument("--exists", action="append", default=[])
    parser.add_argument("--missing", action="append", default=[])
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("pudding-table-check")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    try:
        frame = spark.read.format(args.format).load(args.path)
        frame.createOrReplaceTempView("checked_table")

        if args.count is not None:
            actual = spark.sql("select count(*) as n from checked_table").collect()[0]["n"]
            if actual != args.count:
                fail(f"{args.format} row count {actual} != {args.count}")
        for predicate in args.exists:
            rows = spark.sql(
                f"select 1 from checked_table where {predicate} limit 1").count()
            if rows == 0:
                fail(f"no row matches: {predicate}")
        for predicate in args.missing:
            rows = spark.sql(
                f"select 1 from checked_table where {predicate} limit 1").count()
            if rows != 0:
                fail(f"unexpected row matches: {predicate}")
    finally:
        spark.stop()

    print("ASSERT PASS")


if __name__ == "__main__":
    main()
