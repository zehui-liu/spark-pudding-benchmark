#!/usr/bin/env python3
"""Bulk-load generated JSONL files into Elasticsearch using the HTTP API."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA = ROOT / "data" / "generated"

MAPPINGS = {
    "orders": {
        "order_id": {"type": "long"},
        "customer_id": {"type": "long"},
        "amount": {"type": "double"},
        "status": {"type": "keyword"},
        "city": {"type": "keyword"},
        "order_time": {"type": "date"},
        "updated_at": {"type": "date"},
        "payload": {"type": "keyword", "index": False},
    },
    "customers": {
        "customer_id": {"type": "long"},
        "name": {"type": "keyword"},
        "city": {"type": "keyword"},
        "segment": {"type": "keyword"},
        "updated_at": {"type": "date"},
        "payload": {"type": "keyword", "index": False},
    },
    "payments": {
        "payment_id": {"type": "long"},
        "order_id": {"type": "long"},
        "paid_amount": {"type": "double"},
        "payment_time": {"type": "date"},
        "updated_at": {"type": "date"},
        "payload": {"type": "keyword", "index": False},
    },
}

ID_FIELDS = {
    "orders": "order_id",
    "customers": "customer_id",
    "payments": "payment_id",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:19200")
    parser.add_argument("--tables", default="orders,customers,payments")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--recreate", action="store_true", default=True)
    parser.add_argument("--no-recreate", action="store_false", dest="recreate")
    return parser.parse_args()


def request(method: str, url: str, body: bytes | None = None, content_type: str = "application/json"):
    headers = {"Content-Type": content_type}
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        if error.code == 404:
            return 404, detail.encode()
        raise SystemExit(f"{method} {url} failed ({error.code}): {detail}")


def recreate_index(url: str, table: str) -> None:
    request("DELETE", f"{url}/{table}")
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            # Bulk loading does not need visible segments; restore 1s at the end.
            "refresh_interval": "-1",
        },
        "mappings": {"properties": MAPPINGS[table]},
    }
    status, body = request("PUT", f"{url}/{table}", json.dumps(mapping).encode())
    if status not in (200, 201):
        raise SystemExit(f"failed to create index {table}: {body.decode(errors='replace')}")
    print(f"[{table}] index created", flush=True)


def load_table(url: str, table: str, path: Path, batch_size: int, recreate: bool) -> int:
    if not path.exists():
        raise SystemExit(f"missing dataset: {path}")
    if recreate:
        recreate_index(url, table)

    id_field = ID_FIELDS[table]
    index_url = f"{url}/{table}/_bulk?refresh=false"
    sent = 0
    chunk: list[str] = []

    def flush() -> None:
        nonlocal sent
        if not chunk:
            return
        payload = ("\n".join(chunk) + "\n").encode("utf-8")
        status, body = request(
            "POST", index_url, payload, "application/x-ndjson"
        )
        if status not in (200, 201):
            raise SystemExit(f"bulk request failed ({status}): {body.decode(errors='replace')}")
        response = json.loads(body)
        if response.get("errors"):
            first = next(
                (item for item in response.get("items", []) if item.get("index", {}).get("error")),
                None,
            )
            raise SystemExit(f"bulk indexing reported errors; first: {first}")
        sent += len(chunk) // 2
        chunk.clear()

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            document = json.loads(line)
            chunk.append(json.dumps({"index": {"_index": table, "_id": str(document[id_field])}}))
            chunk.append(line.rstrip("\n"))
            if len(chunk) >= batch_size * 2:
                flush()
                if sent % 500_000 == 0:
                    print(f"[{table}] {sent:,} documents", flush=True)
        flush()

    request(
        "PUT",
        f"{url}/{table}/_settings",
        json.dumps({"index": {"refresh_interval": "1s"}}).encode(),
    )
    request("POST", f"{url}/{table}/_refresh")
    status, body = request("GET", f"{url}/{table}/_count")
    count = json.loads(body).get("count", 0) if status == 200 else -1
    print(f"[{table}] {sent:,} documents submitted, index count={count:,}", flush=True)
    return count


def main() -> int:
    args = parse_args()
    tables = [item.strip() for item in args.tables.split(",") if item.strip()]
    for table in tables:
        load_table(
            args.url.rstrip("/"),
            table,
            args.data_dir / f"{table}.jsonl",
            args.batch_size,
            args.recreate,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
