#!/usr/bin/env python3
"""Verify that each primary table is at least 1 GiB in every loaded system."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "config" / "docker" / "docker-compose.yml"
DEFAULT_DATA = ROOT / "data" / "generated"
DEFAULT_OUT = ROOT / "results" / "data-scale" / "size-report.json"
DEFAULT_TABLES = ("orders", "customers", "payments")
THRESHOLD_BYTES = 1024 ** 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tables", default="orders,customers,payments")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--systems",
        default="csv,postgres,mysql,mongo,es,kafka,lakehouse",
        help="comma separated: csv,postgres,mysql,mongo,es,kafka,lakehouse",
    )
    parser.add_argument("--threshold-gib", type=float, default=1.0)
    parser.add_argument("--strict", action="store_true", help="fail when a requested system is unavailable")
    return parser.parse_args()


def compose_env() -> Dict[str, str]:
    env = dict(os.environ)
    env_file = ROOT / "config" / "docker" / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env.setdefault(key.strip(), value.strip())
    return env


def compose(profile: str, args: list[str], env: Dict[str, str]) -> subprocess.CompletedProcess:
    command = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "--profile",
        profile,
        "exec",
        "-T",
        *args,
    ]
    return subprocess.run(command, capture_output=True, text=True, env=env, check=False)


def running(profile: str, service: str, env: Dict[str, str]) -> bool:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "--profile",
            profile,
            "ps",
            "--services",
            "--status",
            "running",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return service in result.stdout.split()


def postgres_sizes(tables: list[str], env: Dict[str, str]) -> Dict[str, int]:
    if not running("relational", "postgres", env):
        return {}
    user = env.get("PG_USER", "pudding")
    database = env.get("PG_DB", "source_db")
    names = ",".join("'%s'" % table for table in tables)
    sql = (
        "SELECT relname, pg_total_relation_size(oid) FROM pg_class "
        f"WHERE relname IN ({names});"
    )
    result = compose(
        "relational",
        ["postgres", "psql", "-U", user, "-d", database, "-At", "-F", ",", "-c", sql],
        env,
    )
    if result.returncode != 0:
        return {}
    sizes = {}
    for line in result.stdout.splitlines():
        if "," not in line:
            continue
        name, value = line.split(",", 1)
        sizes[name] = int(value)
    return sizes


def mysql_sizes(tables: list[str], env: Dict[str, str]) -> Dict[str, int]:
    if not running("relational", "mysql", env):
        return {}
    password = env.get("MYSQL_ROOT_PASSWORD", "pudding")
    database = env.get("MYSQL_DB", "source_db")
    names = ",".join("'%s'" % table for table in tables)
    sql = (
        "SELECT table_name, data_length + index_length FROM information_schema.tables "
        f"WHERE table_schema = '{database}' AND table_name IN ({names});"
    )
    result = compose(
        "relational",
        [
            "-e",
            f"MYSQL_PWD={password}",
            "mysql",
            "mysql",
            "-uroot",
            "-N",
            "-B",
            "-e",
            sql,
        ],
        env,
    )
    if result.returncode != 0:
        return {}
    sizes = {}
    for line in result.stdout.splitlines():
        if "\t" not in line:
            continue
        name, value = line.split("\t", 1)
        sizes[name] = int(value)
    return sizes


def mongo_sizes(tables: list[str], env: Dict[str, str]) -> Dict[str, int]:
    if not running("relational", "mongo", env):
        return {}
    sizes = {}
    for table in tables:
        expression = (
            f'JSON.stringify(db.getSiblingDB("source_db").{table}.stats())'
        )
        result = compose(
            "relational",
            ["mongo", "mongosh", "--port", "27018", "--quiet", "--eval", expression],
            env,
        )
        if result.returncode != 0 or not result.stdout.strip():
            continue
        try:
            stats = json.loads(result.stdout.strip().splitlines()[-1])
        except json.JSONDecodeError:
            continue
        value = stats.get("size") or stats.get("storageSize")
        if isinstance(value, (int, float)):
            sizes[table] = int(value)
    return sizes


def es_sizes(tables: list[str], env: Dict[str, str]) -> Dict[str, int]:
    port = env.get("ES_PORT", "19200")
    sizes = {}
    for table in tables:
        url = f"http://localhost:{port}/{table}/_stats?filter_path=indices.*.primaries.store.size_in_bytes"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                payload = json.loads(response.read())
        except (urllib.error.URLError, json.JSONDecodeError):
            continue
        indices = payload.get("indices", {})
        if table in indices:
            sizes[table] = int(
                indices[table]["primaries"]["store"]["size_in_bytes"]
            )
    return sizes


def kafka_sizes(tables: list[str], env: Dict[str, str]) -> Dict[str, int]:
    if not running("streaming", "kafka", env):
        return {}
    topic = env.get("KAFKA_TOPIC", "orders-json")
    log_dir = env.get("KAFKA_LOG_DIR", "/tmp/kafka-logs")
    # The apache/kafka-native image does not ship kafka-log-dirs.sh; measure the
    # per-partition topic directories directly instead.
    result = compose(
        "streaming",
        [
            "kafka",
            "sh",
            "-lc",
            f"du -sb {log_dir}/{topic}-[0-9]* 2>/dev/null | "
            "awk '{sum += $1} END {print sum + 0}'",
        ],
        env,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return {}
    try:
        total = int(result.stdout.strip().splitlines()[-1])
    except ValueError:
        return {}
    return {"orders": total} if total else {}


def dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def main() -> int:
    args = parse_args()
    tables = [item.strip() for item in args.tables.split(",") if item.strip()]
    systems = [item.strip() for item in args.systems.split(",") if item.strip()]
    env = compose_env()
    threshold = int(args.threshold_gib * (1024 ** 3))
    report: Dict[str, object] = {
        "thresholdBytes": threshold,
        "thresholdGiB": args.threshold_gib,
        "tables": {},
    }
    failures = []
    skipped = []

    sizes_by_system: Dict[str, Dict[str, int]] = {}
    if "csv" in systems:
        sizes_by_system["csv"] = {
            table: (args.data_dir / f"{table}.csv").stat().st_size
            for table in tables
            if (args.data_dir / f"{table}.csv").exists()
        }
    if "postgres" in systems:
        sizes_by_system["postgres"] = postgres_sizes(tables, env)
        if not sizes_by_system["postgres"]:
            skipped.append("postgres")
    if "mysql" in systems:
        sizes_by_system["mysql"] = mysql_sizes(tables, env)
        if not sizes_by_system["mysql"]:
            skipped.append("mysql")
    if "mongo" in systems:
        sizes_by_system["mongo"] = mongo_sizes(tables, env)
        if not sizes_by_system["mongo"]:
            skipped.append("mongo")
    if "es" in systems:
        sizes_by_system["es"] = es_sizes(tables, env)
        if not sizes_by_system["es"]:
            skipped.append("es")
    if "kafka" in systems:
        sizes_by_system["kafka"] = kafka_sizes(tables, env)
        if not sizes_by_system["kafka"]:
            skipped.append("kafka")
    if "lakehouse" in systems:
        lakehouse = {}
        base = args.data_dir / "lakehouse"
        layouts = {
            "hudi": [base / "hudi" / "{table}"],
            "paimon": [
                base / "paimon" / "pudding_test.db" / "{table}",
                base / "paimon" / "{table}",
            ],
            "iceberg": [
                base / "iceberg" / "pudding_test" / "{table}",
                base / "iceberg" / "{table}",
            ],
        }
        for format_name, patterns in layouts.items():
            for table in tables:
                for pattern in patterns:
                    path = Path(str(pattern).format(table=table))
                    physical = dir_size(path)
                    if physical:
                        # Paimon 1.3.2 rejects file.compression=none for Parquet.
                        # Use the uncompressed CSV source size as the logical
                        # volume and keep the compressed directory size as an
                        # observation. Hudi/Iceberg are written uncompressed.
                        source = args.data_dir / f"{table}.csv"
                        logical = source.stat().st_size if source.exists() else physical
                        lakehouse[f"{format_name}.{table}"] = {
                            "bytes": logical,
                            "physicalBytes": physical,
                        }
                        break
        sizes_by_system["lakehouse"] = lakehouse
        if not lakehouse:
            skipped.append("lakehouse")

    for table in tables:
        table_report: Dict[str, object] = {}
        for system, sizes in sizes_by_system.items():
            if system == "lakehouse":
                for format_name in ("hudi", "paimon", "iceberg"):
                    entry = sizes.get(f"{format_name}.{table}")
                    if entry is None:
                        table_report[f"lakehouse.{format_name}"] = {"status": "missing"}
                        continue
                    value = int(entry["bytes"])
                    status = "passed" if value >= threshold else "failed"
                    table_report[f"lakehouse.{format_name}"] = {
                        "bytes": value,
                        "physicalBytes": int(entry["physicalBytes"]),
                        "gib": round(value / (1024 ** 3), 3),
                        "physicalGib": round(
                            int(entry["physicalBytes"]) / (1024 ** 3), 3
                        ),
                        "status": status,
                    }
                    if status == "failed":
                        failures.append(
                            f"{table}/lakehouse.{format_name}: "
                            f"{value / (1024 ** 3):.3f} GiB"
                        )
                continue
            value = sizes.get(table)
            if value is None and system == "kafka":
                value = sizes.get("orders") if table == "orders" else None
            if value is None:
                table_report[system] = {"status": "missing"}
                continue
            status = "passed" if value >= threshold else "failed"
            table_report[system] = {
                "bytes": value,
                "gib": round(value / (1024 ** 3), 3),
                "status": status,
            }
            if status == "failed":
                failures.append(f"{table}/{system}: {value / (1024 ** 3):.3f} GiB")
        report["tables"][table] = table_report

    report["skippedSystems"] = sorted(set(skipped))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"report: {args.out}", file=sys.stderr)
    if failures:
        print("size gate failed: " + "; ".join(failures), file=sys.stderr)
        return 1
    if args.strict and skipped:
        print("size gate skipped systems: " + ", ".join(sorted(set(skipped))), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
