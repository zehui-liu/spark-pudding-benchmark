#!/usr/bin/env python3
"""Generate the sync-full job templates for E2E-14.

Each template reads the same CSV through a limit transform and writes to one
sink. `run-e2e.sh E2E-14` runs each template twice with E2E14_LIMIT=100 and then
200; the second run must leave exactly 200 rows, proving replacement rather than
append.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

ROOT = Path(__file__).resolve().parents[2]
JOBS = ROOT / "config" / "jobs"

COMMON_CONFIG = {
    "spark.ui.enabled": "false",
    "spark.sql.shuffle.partitions": "4",
    "spark.driver.memory": "2g",
    "spark.driver.cores": "2",
}

HIVE_CONFIG = {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "${PUDDING_TEST_ROOT}/data/hive/warehouse",
    "spark.hadoop.javax.jdo.option.ConnectionURL":
        "jdbc:derby:;databaseName=${PUDDING_TEST_ROOT}/data/hive/metastore_db;create=true",
}

ORDERS_SCHEMA = {
    "order_id": "long",
    "customer_id": "long",
    "amount": "double",
    "status": "string",
    "city": "string",
    "order_time": "timestamp",
    "updated_at": "timestamp",
    "payload": "string",
}


def base_job(name: str, description: str, extra_config: Dict[str, str] | None = None) -> Dict[str, Any]:
    config = dict(COMMON_CONFIG)
    if extra_config:
        config.update(extra_config)
    return {
        "job": {
            "mode": "sync",
            "strategy": "full",
            "sparkHome": "${SPARK_HOME}",
            "sparkPuddingHome": "${PUDDING_HOME}",
            "appName": f"e2e-14-{name}-full-refresh",
            "master": "local[2]",
            "deployMode": "client",
            "describe": description,
            "config": config,
        }
    }


def source_transform() -> Dict[str, Any]:
    return {
        "source": [
            {
                "type": "file-common-source",
                "id": "source-1",
                "config": {
                    "path": "${PUDDING_TEST_DATA}/orders.csv",
                    "fileType": "csv",
                    "streamRead": False,
                    "forceConvertSchema": True,
                    "options": {"header": "true", "delimiter": ","},
                    "schema": ORDERS_SCHEMA,
                },
            }
        ],
        "transform": [
            {
                "type": "limit-transform",
                "id": "limit-rows",
                "dependencies": ["source-1"],
                "config": {"limit": "${E2E14_LIMIT}"},
            }
        ],
    }


def sink(
    job: Dict[str, Any],
    sink_type: str,
    config: Dict[str, Any],
    catalog: Any = None,
    catalog_name: str | None = None,
) -> Dict[str, Any]:
    job.update(source_transform())
    if catalog is not None:
        job["catalog"] = [catalog]
    entry: Dict[str, Any] = {
        "type": sink_type,
        "id": "sink-1",
        "dependency": "limit-rows",
        "config": config,
    }
    if catalog_name is not None:
        entry["catalog"] = catalog_name
    job["sink"] = [entry]
    return job


def write(name: str, job: Dict[str, Any]) -> None:
    path = JOBS / name
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(job, handle, sort_keys=False, allow_unicode=True)
    print(f"wrote {path.relative_to(ROOT)}")


def main() -> None:
    file_job = base_job("file", "CSV to file full refresh")
    write("E2E-14-file-full-refresh.yaml", sink(file_job, "file-sink", {
        "path": "${PUDDING_TEST_ROOT}/results/state/e2e-14/file",
        "fileType": "json",
        "saveMode": "overwrite",
        "allowNonAtomicReplace": True,
    }))

    hive_job = base_job("hive", "CSV to Hive full refresh", HIVE_CONFIG)
    write("E2E-14-hive-full-refresh.yaml", sink(hive_job, "hive-sink", {
        "table": "default.pudding_e2e14_hive",
        "mode": "overwrite",
        "allowNonAtomicReplace": True,
    }))

    jdbc_job = base_job("jdbc", "CSV to PostgreSQL full refresh")
    write("E2E-14-jdbc-full-refresh.yaml", sink(jdbc_job, "jdbc-sink", {
        "url": "${PG_TARGET_URL}",
        "user": "${PG_USER}",
        "password": "${PG_PASSWORD}",
        "table": "public.e2e14_orders",
        "driver": "org.postgresql.Driver",
    }))

    mysql_job = base_job("mysql", "CSV to MySQL full refresh")
    write("E2E-14-mysql-full-refresh.yaml", sink(mysql_job, "mysql-sink", {
        "url": "${MYSQL_TARGET_URL}",
        "user": "${MYSQL_USER}",
        "password": "${MYSQL_PASSWORD}",
        "table": "e2e14_orders",
        "driver": "com.mysql.cj.jdbc.Driver",
    }))

    hudi_job = base_job("hudi", "CSV to Hudi full refresh", {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    })
    write("E2E-14-hudi-full-refresh.yaml", sink(hudi_job, "hudi-sink", {
        "table": "e2e14_orders",
        "tablePath": "${PUDDING_TEST_ROOT}/results/state/e2e-14/hudi/orders",
        "tableType": "COPY_ON_WRITE",
        "recordKeyFields": ["order_id"],
        "preCombineField": "updated_at",
    }))

    paimon_job = base_job("paimon", "CSV to Paimon full refresh")
    write("E2E-14-paimon-full-refresh.yaml", sink(paimon_job, "paimon-sink", {
        "table": "pudding_test.e2e14_orders",
        "primaryKey": ["order_id"],
        "bucket": 1,
        # Paimon replaces tables with delete+insert rather than one atomic staged
        # commit, so the sink requires the explicit non-atomic opt-in.
        "allowNonAtomicReplace": True,
    }, catalog={
        "name": "paimon_lake",
        "type": "paimon",
        "properties": {
            "warehouse": "${PUDDING_TEST_ROOT}/results/state/e2e-14/paimon",
            "metastore": "filesystem",
        },
    }, catalog_name="paimon_lake"))

    iceberg_job = base_job("iceberg", "CSV to Iceberg full refresh")
    write("E2E-14-iceberg-full-refresh.yaml", sink(iceberg_job, "iceberg-sink", {
        "table": "pudding_test.e2e14_orders",
    }, catalog={
        "name": "iceberg_lake",
        "type": "iceberg",
        "properties": {
            "type": "hadoop",
            "warehouse": "${PUDDING_TEST_ROOT}/data/generated/lakehouse/iceberg",
        },
    }, catalog_name="iceberg_lake"))


if __name__ == "__main__":
    main()
