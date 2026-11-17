#!/usr/bin/env python3
"""Load generated JSONL files into MongoDB and initialize a single-node replica set."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover - exercised by the operator
    print(
        "pymongo is required: python3 -m pip install -r scripts/data-gen/requirements.txt",
        file=sys.stderr,
    )
    raise SystemExit(2)

DEFAULT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA = DEFAULT_ROOT / "data" / "generated"

INDEXES = {
    "orders": [
        ("order_id", True),
        ("updated_at", False),
        ("customer_id", False),
    ],
    "customers": [
        ("customer_id", True),
        ("updated_at", False),
    ],
    "payments": [
        ("payment_id", True),
        ("order_id", False),
        ("updated_at", False),
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=27018)
    parser.add_argument("--database", default="source_db")
    parser.add_argument("--tables", default="orders,customers,payments")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--drop", action="store_true", default=True)
    parser.add_argument("--no-drop", action="store_false", dest="drop")
    return parser.parse_args()


def initiate_replica_set(client: MongoClient, host: str, port: int) -> None:
    try:
        hello = client.admin.command("hello")
    except PyMongoError as error:
        raise SystemExit(f"cannot reach MongoDB at {host}:{port}: {error}")

    if hello.get("setName") == "rs0":
        return

    member = f"{host}:{port}"
    print(f"initiating MongoDB replica set rs0 with member {member}", flush=True)
    try:
        client.admin.command(
            "replSetInitiate",
            {"_id": "rs0", "members": [{"_id": 0, "host": member}]},
        )
    except PyMongoError as error:
        # Another process may have initiated it between hello and initiate.
        if "already initialized" not in str(error):
            raise

    deadline = time.time() + 120
    while time.time() < deadline:
        hello = client.admin.command("hello")
        if hello.get("isWritablePrimary"):
            print("MongoDB replica set is primary", flush=True)
            return
        time.sleep(2)
    raise SystemExit("MongoDB replica set did not become primary within 120s")


def load_table(client: MongoClient, database: str, table: str, path: Path, batch_size: int) -> int:
    collection = client[database][table]
    if path.suffix == ".jsonl":
        pass
    else:
        raise SystemExit(f"expected JSONL input, got: {path}")

    if not path.exists():
        raise SystemExit(f"missing dataset: {path}")

    collection.drop()
    inserted = 0
    batch = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
                batch.clear()
                if inserted % 500_000 == 0:
                    print(f"[{table}] {inserted:,} documents", flush=True)
        if batch:
            collection.insert_many(batch, ordered=False)
            inserted += len(batch)

    for field, unique in INDEXES.get(table, []):
        collection.create_index([(field, ASCENDING)], unique=unique)
    print(f"[{table}] {inserted:,} documents loaded", flush=True)
    return inserted


def main() -> int:
    args = parse_args()
    direct_uri = f"mongodb://{args.host}:{args.port}/?directConnection=true"
    client = MongoClient(direct_uri, serverSelectionTimeoutMS=5000)
    initiate_replica_set(client, args.host, args.port)
    client.close()

    uri = f"mongodb://{args.host}:{args.port}/?replicaSet=rs0"
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)

    tables = [item.strip() for item in args.tables.split(",") if item.strip()]
    for table in tables:
        load_table(client, args.database, table, args.data_dir / f"{table}.jsonl", args.batch_size)

    print("document counts:")
    for table in tables:
        print(f"  {table}: {client[args.database][table].count_documents({}):,}")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
