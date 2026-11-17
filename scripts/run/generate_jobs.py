#!/usr/bin/env python3
"""Generate the component smoke job templates in config/jobs/.

The generated files use ${...} placeholders that scripts/run/run-case.sh renders
with envsubst. Re-run this script after editing a component definition.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict

import yaml

ROOT = Path(__file__).resolve().parents[2]
JOBS = ROOT / "config" / "jobs"

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
CUSTOMERS_SCHEMA = {
    "customer_id": "long",
    "name": "string",
    "city": "string",
    "segment": "string",
    "updated_at": "timestamp",
    "payload": "string",
}
PAYMENTS_SCHEMA = {
    "payment_id": "long",
    "order_id": "long",
    "paid_amount": "double",
    "payment_time": "timestamp",
    "updated_at": "timestamp",
    "payload": "string",
}
SCHEMAS = {"orders": ORDERS_SCHEMA, "customers": CUSTOMERS_SCHEMA, "payments": PAYMENTS_SCHEMA}

COMMON_CONFIG = {
    "spark.ui.enabled": "false",
    "spark.sql.shuffle.partitions": "4",
    "spark.driver.memory": "2g",
    "spark.driver.cores": "2",
}

HIVE_CONFIG = {
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.warehouse.dir": "${PUDDING_TEST_ROOT}/data/hive/warehouse",
    "spark.hadoop.javax.jdo.option.ConnectionURL": (
        "jdbc:derby:;databaseName=${PUDDING_TEST_ROOT}/data/hive/metastore_db;create=true"
    ),
}


def base_job(
    case_id: str,
    description: str,
    mode: str = "batch",
    trigger: str | None = None,
    config: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    job: Dict[str, Any] = {
        "mode": mode,
        "sparkHome": "${SPARK_HOME}",
        "sparkPuddingHome": "${PUDDING_HOME}",
        "appName": f"{case_id.lower()}-{description}",
        "master": "local[2]",
        "deployMode": "client",
        "describe": description,
        "config": dict(COMMON_CONFIG),
    }
    if config:
        job["config"].update(config)
    if trigger:
        job["trigger"] = trigger
    return {"job": job}


def file_source(
    source_id: str = "source-1",
    table: str = "orders",
    path: str | None = None,
) -> Dict[str, Any]:
    return {
        "type": "file-common-source",
        "id": source_id,
        "config": {
            "path": path or f"${{PUDDING_TEST_DATA}}/{table}.csv",
            "fileType": "csv",
            "streamRead": False,
            "forceConvertSchema": True,
            "options": {"header": "true", "delimiter": ","},
            "schema": dict(SCHEMAS[table]),
        },
    }


def file_sink(dependency: str = "source-1", suffix: str = "result") -> Dict[str, Any]:
    return {
        "type": "file-sink",
        "id": "sink-1",
        "dependency": dependency,
        "config": {
            "path": f"${{CASE_OUT}}/{suffix}",
            "fileType": "json",
            "saveMode": "overwrite",
        },
    }


def console_sink(dependency: str = "source-1") -> Dict[str, Any]:
    return {
        "type": "console-sink",
        "id": "sink-1",
        "dependency": dependency,
        "config": {
            "isStreaming": False,
            "numRows": 20,
            "truncate": 50,
            "vertical": False,
            "printSchema": True,
        },
    }


def write_job(filename: str, document: Dict[str, Any]) -> None:
    JOBS.mkdir(parents=True, exist_ok=True)
    path = JOBS / filename
    path.write_text(
        yaml.safe_dump(document, sort_keys=False, allow_unicode=True, width=1000)
    )
    print(f"wrote {path.relative_to(ROOT)}")


def source_job(case_id: str, source: Dict[str, Any], suffix: str = "source") -> Dict[str, Any]:
    document = base_job(case_id, f"{source['type']} smoke")
    document["source"] = [source]
    document["sink"] = [file_sink(source["id"], suffix)]
    return document


def add_source_jobs() -> None:
    write_job("SR-01-file-source.yaml", source_job("SR-01", file_source()))

    jdbc_source = {
        "type": "jdbc-source",
        "id": "source-1",
        "config": {
            "url": "${PG_URL}",
            "table": "(SELECT * FROM public.orders LIMIT 10000) AS smoke_orders",
            "user": "${PG_USER}",
            "password": "${PG_PASSWORD}",
            "driver": "org.postgresql.Driver",
            "options": {"fetchsize": "1000"},
        },
    }
    write_job("SR-02-jdbc-source.yaml", source_job("SR-02", jdbc_source))

    mysql_source = {
        "type": "mysql-source",
        "id": "source-1",
        "config": {
            "url": "${MYSQL_URL}",
            "table": "(SELECT * FROM orders LIMIT 10000) AS smoke_orders",
            "user": "${MYSQL_USER}",
            "password": "${MYSQL_PASSWORD}",
            "driver": "com.mysql.cj.jdbc.Driver",
        },
    }
    write_job("SR-03-mysql-source.yaml", source_job("SR-03", mysql_source))

    mongodb_source = {
        "type": "mongodb-source",
        "id": "source-1",
        "config": {
            "uri": "${MONGO_URI}",
            "database": "source_db",
            "collection": "orders_smoke",
            "options": {"readPreference.name": "primary"},
        },
    }
    write_job("SR-04-mongodb-source.yaml", source_job("SR-04", mongodb_source))

    es_source = {
        "type": "elasticsearch-source",
        "id": "source-1",
        "config": {
            "esNodes": "localhost",
            "esPort": "${ES_PORT}",
            "esIndex": "orders",
            "limit": 10000,
            "scrollSize": 1000,
            "options": {"es.nodes.wan.only": "true"},
        },
    }
    write_job("SR-05-elasticsearch-source.yaml", source_job("SR-05", es_source))

    kafka_source = {
        "type": "kafka-source",
        "id": "source-1",
        "config": {
            "kafkaBootstrapServers": "${KAFKA_BOOTSTRAP}",
            "subscribe": "orders-smoke",
            "valueFormat": "json",
            "valueSchema": dict(ORDERS_SCHEMA),
            "options": {
                "startingOffsets": "earliest",
                "endingOffsets": "latest",
            },
        },
    }
    write_job("SR-06-kafka-source.yaml", source_job("SR-06", kafka_source))

    hive_source = {
        "type": "hive-source",
        "id": "source-1",
        "config": {"table": "default.pudding_orders_hive"},
    }
    hive_job = source_job("SR-07", hive_source)
    hive_job["job"]["config"].update(HIVE_CONFIG)
    write_job("SR-07-hive-source.yaml", hive_job)

    hudi_source = {
        "type": "hudi-source",
        "id": "source-1",
        "config": {
            "table": "orders",
            "tablePath": "${PUDDING_TEST_ROOT}/data/generated/lakehouse/hudi/orders",
            "filterExpr": "order_id <= 10000",
        },
    }
    write_job("SR-08-hudi-source.yaml", source_job("SR-08", hudi_source))

    paimon_source = {
        "type": "paimon-source",
        "id": "source-1",
        "catalog": "paimon_lake",
        "config": {
            "table": "pudding_test.orders",
            "scanMode": "latest-full",
            "filterExpr": "order_id <= 10000",
        },
    }
    paimon_job = source_job("SR-09", paimon_source)
    paimon_job["catalog"] = [
        {
            "name": "paimon_lake",
            "type": "paimon",
            "properties": {
                "warehouse": "${PUDDING_TEST_ROOT}/data/generated/lakehouse/paimon",
                "metastore": "filesystem",
            },
        }
    ]
    write_job("SR-09-paimon-source.yaml", paimon_job)

    iceberg_source = {
        "type": "iceberg-source",
        "id": "source-1",
        "catalog": "iceberg_lake",
        "config": {
            "table": "pudding_test.orders",
            "filterExpr": "order_id <= 10000",
        },
    }
    iceberg_job = source_job("SR-10", iceberg_source)
    iceberg_job["catalog"] = [
        {
            "name": "iceberg_lake",
            "type": "iceberg",
            "properties": {
                "type": "hadoop",
                "warehouse": "${PUDDING_TEST_ROOT}/data/generated/lakehouse/iceberg",
            },
        }
    ]
    write_job("SR-10-iceberg-source.yaml", iceberg_job)

    # CDC templates are streaming-only. They are kept for the CDC phase; the
    # smoke runner does not include them in the P0 batch set.
    mysql_cdc = {
        "type": "mysql-cdc-source",
        "id": "source-1",
        "config": {
            "url": "${MYSQL_URL}",
            "user": "${MYSQL_USER}",
            "password": "${MYSQL_PASSWORD}",
            "database": "source_db",
            "table": "orders",
            "schema": (
                "order_id bigint, customer_id bigint, amount decimal(12,2), "
                "status string, city string, order_time timestamp, updated_at timestamp, "
                "payload string"
            ),
            "snapshotMode": "schema_only",
            "maxRecordsPerBatch": "1000",
            "options": {"database.allowPublicKeyRetrieval": "true"},
        },
    }
    cdc_job = base_job("SR-11", "mysql-cdc-source smoke", mode="streaming",
                       trigger="processing-time: 5 seconds")
    cdc_job["source"] = [mysql_cdc]
    cdc_job["sink"] = [{
        "type": "file-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "path": "${CASE_OUT}/cdc",
            "fileType": "json",
            "isStreaming": True,
            "outputMode": "append",
            "checkpointLocation": "${CASE_OUT}/checkpoint",
            "checkpointRetention": "keep",
        },
    }]
    write_job("SR-11-mysql-cdc-source.yaml", cdc_job)

    postgres_cdc = {
        "type": "postgres-cdc-source",
        "id": "source-1",
        "config": {
            "url": "${PG_URL}",
            "user": "${PG_USER}",
            "password": "${PG_PASSWORD}",
            "database": "source_db",
            "databaseSchema": "public",
            # 10k-row copy: exercises the initial snapshot without replaying
            # the 10M-row scale table on every smoke run.
            "table": "orders_smoke",
            "schema": (
                "order_id bigint, customer_id bigint, amount decimal(12,2), "
                "status string, city string, order_time timestamp, updated_at timestamp, "
                "payload string"
            ),
            "snapshotMode": "initial",
            "maxRecordsPerBatch": "1000",
        },
    }
    cdc_job = base_job("SR-12", "postgres-cdc-source smoke", mode="streaming",
                       trigger="processing-time: 10 seconds")
    cdc_job["source"] = [postgres_cdc]
    cdc_job["sink"] = [{
        "type": "file-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "path": "${CASE_OUT}/cdc",
            "fileType": "json",
            "isStreaming": True,
            "outputMode": "append",
            "checkpointLocation": "${CASE_OUT}/checkpoint",
            "checkpointRetention": "keep",
        },
    }]
    write_job("SR-12-postgres-cdc-source.yaml", cdc_job)

    mongodb_cdc = {
        "type": "mongodb-cdc-source",
        "id": "source-1",
        "config": {
            "uri": "${MONGO_URI}",
            "database": "source_db",
            "collection": "orders_smoke",
            # MongoDB delete events only carry the document key, so the CDC
            # schema must declare _id; it is encoded as the ObjectId hex string.
            "schema": (
                "_id string, order_id bigint, customer_id bigint, amount double, "
                "status string, city string, order_time timestamp, updated_at timestamp, "
                "payload string"
            ),
            "snapshotMode": "initial",
            # 10k snapshot rows with a 2s trigger drain quickly; the default
            # 10s trigger would need ten micro-batches before live changes.
            "maxRecordsPerBatch": "2000",
        },
    }
    cdc_job = base_job("SR-13", "mongodb-cdc-source smoke", mode="streaming",
                       trigger="processing-time: 2 seconds")
    cdc_job["source"] = [mongodb_cdc]
    cdc_job["sink"] = [{
        "type": "file-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "path": "${CASE_OUT}/cdc",
            "fileType": "json",
            "isStreaming": True,
            "outputMode": "append",
            "checkpointLocation": "${CASE_OUT}/checkpoint",
            "checkpointRetention": "keep",
        },
    }]
    write_job("SR-13-mongodb-cdc-source.yaml", cdc_job)


def paimon_cdc_sink(case_id: str) -> Dict[str, Any]:
    return {
        "type": "paimon-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "path": f"${{CASE_OUT}}/paimon/{case_id.lower()}",
            "primaryKey": ["order_id"],
            "bucket": 1,
            "outputMode": "append",
            "checkpointLocation": f"${{CASE_OUT}}/checkpoint",
            "checkpointRetention": "keep",
            "cdcOpColumn": "__op",
            "cdcOrderFields": ["_cdc_timestamp", "_cdc_sequence"],
        },
    }


def iceberg_cdc_sink(case_id: str) -> Dict[str, Any]:
    return {
        "type": "iceberg-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "catalog": "iceberg_lake",
        "config": {
            "table": f"pudding_test.{case_id.lower()}_orders",
            "outputMode": "append",
            "checkpointLocation": f"${{CASE_OUT}}/checkpoint",
            "checkpointRetention": "keep",
            "cdcOpColumn": "__op",
            "cdcOrderFields": ["_cdc_timestamp", "_cdc_sequence"],
        },
    }


def add_sink_jobs() -> None:
    write_job(
        "SK-01-console-sink.yaml",
        {"job": base_job("SK-01", "console-sink smoke")["job"],
         "source": [file_source()], "sink": [console_sink()]},
    )
    write_job(
        "SK-02-file-sink.yaml",
        {"job": base_job("SK-02", "file-sink smoke")["job"],
         "source": [file_source()], "sink": [file_sink()]},
    )

    jdbc_sink = {
        "type": "jdbc-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "url": "${PG_TARGET_URL}",
            "table": "public.orders",
            "user": "${PG_USER}",
            "password": "${PG_PASSWORD}",
            "driver": "org.postgresql.Driver",
            "mode": "overwrite",
            "options": {"batchsize": "1000"},
        },
    }
    write_job("SK-03-jdbc-sink.yaml",
              {"job": base_job("SK-03", "jdbc-sink smoke")["job"],
               "source": [file_source()], "sink": [jdbc_sink]})

    mysql_sink = {
        "type": "mysql-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "url": "${MYSQL_TARGET_URL}",
            "table": "orders",
            "user": "${MYSQL_USER}",
            "password": "${MYSQL_PASSWORD}",
            "driver": "com.mysql.cj.jdbc.Driver",
            "mode": "overwrite",
        },
    }
    write_job("SK-04-mysql-sink.yaml",
              {"job": base_job("SK-04", "mysql-sink smoke")["job"],
               "source": [file_source()], "sink": [mysql_sink]})

    mongo_sink = {
        "type": "mongodb-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "uri": "${MONGO_URI}",
            "database": "source_db",
            "collection": "sk05_orders",
            "operation": "insert",
            "idFields": ["order_id"],
            "mode": "overwrite",
        },
    }
    write_job("SK-05-mongodb-sink.yaml",
              {"job": base_job("SK-05", "mongodb-sink smoke")["job"],
               "source": [file_source()], "sink": [mongo_sink]})

    es_sink = {
        "type": "elasticsearch-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "esNodes": "localhost",
            "esPort": "${ES_PORT}",
            "esIndex": "sk06_orders",
            "operation": "index",
            "idField": "order_id",
            "mode": "overwrite",
            "options": {
                "es.batch.size.entries": "1000",
                "es.nodes.wan.only": "true",
            },
        },
    }
    write_job("SK-06-elasticsearch-sink.yaml",
              {"job": base_job("SK-06", "elasticsearch-sink smoke")["job"],
               "source": [file_source()], "sink": [es_sink]})

    kafka_sink = {
        "type": "kafka-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "kafkaBootstrapServers": "${KAFKA_BOOTSTRAP}",
            "topic": "orders-sink-smoke",
            "valueColumn": "payload",
            "mode": "append",
            "options": {"acks": "all"},
        },
    }
    write_job("SK-07-kafka-sink.yaml",
              {"job": base_job("SK-07", "kafka-sink smoke")["job"],
               "source": [file_source()], "sink": [kafka_sink]})

    hive_job = base_job("SK-08", "hive-sink smoke", config=HIVE_CONFIG)
    hive_job["source"] = [file_source()]
    hive_job["sink"] = [{
        "type": "hive-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {"table": "default.pudding_orders_hive", "mode": "overwrite"},
    }]
    write_job("SK-08-hive-sink.yaml", hive_job)

    hudi_sink = {
        "type": "hudi-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "config": {
            "table": "orders",
            "tablePath": "${CASE_OUT}/hudi/orders",
            "operation": "bulk_insert",
            "tableType": "COPY_ON_WRITE",
            "recordKeyFields": ["order_id"],
            "preCombineField": "updated_at",
            "mode": "append",
        },
    }
    hudi_job = base_job(
        "SK-09",
        "hudi-sink smoke",
        config={"spark.serializer": "org.apache.spark.serializer.KryoSerializer"},
    )
    hudi_job["source"] = [file_source()]
    hudi_job["sink"] = [hudi_sink]
    write_job("SK-09-hudi-sink.yaml", hudi_job)

    paimon_job = base_job("SK-10", "paimon-sink smoke")
    paimon_job["catalog"] = [{
        "name": "paimon_lake",
        "type": "paimon",
        "properties": {
            "warehouse": "${PUDDING_TEST_ROOT}/data/generated/lakehouse/paimon",
            "metastore": "filesystem",
        },
    }]
    paimon_job["source"] = [file_source()]
    paimon_job["sink"] = [{
        "type": "paimon-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "catalog": "paimon_lake",
        "config": {
            "table": "pudding_test.sk10_orders",
            "primaryKey": ["order_id"],
            "bucket": 1,
            "mode": "overwrite",
        },
    }]
    write_job("SK-10-paimon-sink.yaml", paimon_job)

    iceberg_job = base_job("SK-11", "iceberg-sink smoke")
    iceberg_job["catalog"] = [{
        "name": "iceberg_lake",
        "type": "iceberg",
        "properties": {
            "type": "hadoop",
            "warehouse": "${PUDDING_TEST_ROOT}/data/generated/lakehouse/iceberg",
        },
    }]
    iceberg_job["source"] = [file_source()]
    iceberg_job["sink"] = [{
        "type": "iceberg-sink",
        "id": "sink-1",
        "dependency": "source-1",
        "catalog": "iceberg_lake",
        "config": {
            "table": "pudding_test.sk11_orders",
            "mode": "append",
        },
    }]
    write_job("SK-11-iceberg-sink.yaml", iceberg_job)


def transform_job(case_id: str, transforms: list[dict], sources: list[dict] | None = None) -> Dict[str, Any]:
    document = base_job(case_id, "transform smoke")
    document["source"] = sources or [file_source()]
    document["transform"] = transforms
    document["sink"] = [file_sink(transforms[-1]["id"])]
    return document


def add_transform_jobs() -> None:
    write_job(
        "TR-01-filter-transform.yaml",
        transform_job("TR-01", [{
            "type": "filter-transform", "id": "filter1", "dependencies": ["source-1"],
            "config": {"condition": "amount >= 100", "numPartitions": 2},
        }]),
    )
    write_job(
        "TR-02-aggregate-transform.yaml",
        transform_job("TR-02", [{
            "type": "aggregate-transform", "id": "aggregate1", "dependencies": ["source-1"],
            "config": {
                "groupBy": ["city"],
                "aggregations": {
                    "order_count": "count(*)",
                    "total_amount": "sum(amount)",
                },
            },
        }]),
    )
    write_job(
        "TR-03-union-transform.yaml",
        transform_job("TR-03", [{
            "type": "union-transform", "id": "union1",
            "dependencies": ["source-1", "source-2"],
            "config": {
                "inputs": ["source-1", "source-2"],
                "mode": "byName",
                "distinct": False,
            },
        }], sources=[file_source("source-1"), file_source("source-2")]),
    )
    write_job(
        "TR-04-column-transform.yaml",
        transform_job("TR-04", [{
            "type": "column-transform", "id": "column1", "dependencies": ["source-1"],
            "config": {
                "fields": [
                    "order_id",
                    {"name": "city_name", "source": "city", "type": "string"},
                    {"name": "amount", "type": "double"},
                ],
            },
        }]),
    )
    write_job(
        "TR-05-limit-transform.yaml",
        transform_job("TR-05", [{
            "type": "limit-transform", "id": "limit1", "dependencies": ["source-1"],
            "config": {"limit": 100},
        }]),
    )
    write_job(
        "TR-06-join-transform.yaml",
        transform_job("TR-06", [{
            "type": "join-transform", "id": "join1",
            "dependencies": ["source-1", "source-2"],
            "config": {
                "left": "source-1",
                "right": "source-2",
                "joinType": "left_semi",
                "on": ["customer_id"],
            },
        }], sources=[file_source("source-1", "orders"),
                     file_source("source-2", "customers")]),
    )
    write_job(
        "TR-07-validate-transform.yaml",
        transform_job("TR-07", [{
            "type": "validate-transform", "id": "validate1", "dependencies": ["source-1"],
            "config": {
                "requiredColumns": ["order_id", "amount", "city"],
                "rules": {"amount_nonnegative": "amount >= 0"},
                "mode": "drop",
                "quarantinePath": "${CASE_OUT}/quarantine",
            },
        }]),
    )
    write_job(
        "TR-08-dedup-transform.yaml",
        transform_job("TR-08", [{
            "type": "dedup-transform", "id": "dedup1", "dependencies": ["source-1"],
            "config": {"keys": ["order_id"]},
        }]),
    )
    write_job(
        "TR-09-schema-contract-transform.yaml",
        transform_job("TR-09", [{
            "type": "schema-contract-transform", "id": "contract1",
            "dependencies": ["source-1"],
            "config": {
                "columns": dict(ORDERS_SCHEMA),
                "allowExtraColumns": True,
            },
        }]),
    )
    write_job(
        "TR-10-sql-transform.yaml",
        transform_job("TR-10", [{
            "type": "sql-transform", "id": "sql1", "dependencies": ["source-1"],
            "config": {
                "sql": "SELECT order_id, city, amount FROM orders_src WHERE amount >= 100",
                "tempView": "orders_src",
            },
        }]),
    )


def main() -> None:
    JOBS.mkdir(parents=True, exist_ok=True)
    for path in JOBS.glob("SR-*.yaml"):
        path.unlink()
    for path in JOBS.glob("SK-*.yaml"):
        path.unlink()
    for path in JOBS.glob("TR-*.yaml"):
        path.unlink()
    add_source_jobs()
    add_sink_jobs()
    add_transform_jobs()


if __name__ == "__main__":
    main()
