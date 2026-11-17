#!/usr/bin/env python3
"""Shared deterministic row generation for the spark-pudding-benchmark datasets."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = ROOT / "data" / "generated"

MASK64 = (1 << 64) - 1

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
CITIES = (
    "beijing",
    "shanghai",
    "shenzhen",
    "hangzhou",
    "chengdu",
    "wuhan",
    "nanjing",
    "xian",
    "suzhou",
    "tianjin",
    "chongqing",
    "changsha",
    "qingdao",
    "zhengzhou",
    "hefei",
    "fuzhou",
    "xiamen",
    "kunming",
    "dalian",
    "jinan",
)
STATUSES = ("NEW", "PAID", "SHIPPED", "DONE", "CANCELLED")
SEGMENTS = ("VIP", "NORMAL", "NEW")


def payload(index: int, length: int) -> str:
    """Return a deterministic, quote-free payload of the requested length."""
    x = (index * 0x9E3779B97F4A7C15 + 0x2545F4914F6CDD1D) & MASK64
    y = ((index ^ (index >> 31)) * 0xBF58476D1CE4E5B9) & MASK64
    seed = f"{x:016x}{y:016x}"
    return (seed * ((length // len(seed)) + 1))[:length]


def csv_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def json_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: Tuple[str, ...]
    primary_key: str

    def row(self, index: int, total: int) -> Dict[str, object]:
        raise NotImplementedError

    def csv_values(self, row: Dict[str, object]) -> Sequence[object]:
        raise NotImplementedError

    def json_line(self, row: Dict[str, object]) -> str:
        raise NotImplementedError


class Orders(TableSpec):
    def __init__(self) -> None:
        super().__init__(
            name="orders",
            columns=(
                "order_id",
                "customer_id",
                "amount",
                "status",
                "city",
                "order_time",
                "updated_at",
                "payload",
            ),
            primary_key="order_id",
        )

    def row(self, index: int, total: int) -> Dict[str, object]:
        amount_cents = (index * 37) % 1_000_000
        order_time = BASE_TIME + timedelta(seconds=index)
        updated_at = order_time + timedelta(seconds=(index * 17) % 86_400)
        return {
            "order_id": index,
            "customer_id": ((index * 13 - 1) % 1_000_000) + 1,
            "amount_cents": amount_cents,
            "status": STATUSES[index % len(STATUSES)],
            "city": CITIES[(index * 7) % len(CITIES)],
            "order_time": order_time,
            "updated_at": updated_at,
            "payload": payload(index, 64),
        }

    def csv_values(self, row: Dict[str, object]) -> Sequence[object]:
        cents = int(row["amount_cents"])
        return (
            row["order_id"],
            row["customer_id"],
            f"{cents // 100}.{cents % 100:02d}",
            row["status"],
            row["city"],
            csv_time(row["order_time"]),
            csv_time(row["updated_at"]),
            row["payload"],
        )

    def json_line(self, row: Dict[str, object]) -> str:
        cents = int(row["amount_cents"])
        return (
            '{"order_id":%d,"customer_id":%d,"amount":%d.%02d,'
            '"status":"%s","city":"%s","order_time":"%s","updated_at":"%s",'
            '"payload":"%s"}'
            % (
                row["order_id"],
                row["customer_id"],
                cents // 100,
                cents % 100,
                row["status"],
                row["city"],
                json_time(row["order_time"]),
                json_time(row["updated_at"]),
                row["payload"],
            )
        )


class Customers(TableSpec):
    def __init__(self) -> None:
        super().__init__(
            name="customers",
            columns=(
                "customer_id",
                "name",
                "city",
                "segment",
                "updated_at",
                "payload",
            ),
            primary_key="customer_id",
        )

    def row(self, index: int, total: int) -> Dict[str, object]:
        updated_at = BASE_TIME + timedelta(seconds=index * 2)
        return {
            "customer_id": index,
            "name": "customer_%08d" % index,
            "city": CITIES[(index * 11) % len(CITIES)],
            "segment": SEGMENTS[index % len(SEGMENTS)],
            "updated_at": updated_at,
            "payload": payload(index + 7, 96),
        }

    def csv_values(self, row: Dict[str, object]) -> Sequence[object]:
        return (
            row["customer_id"],
            row["name"],
            row["city"],
            row["segment"],
            csv_time(row["updated_at"]),
            row["payload"],
        )

    def json_line(self, row: Dict[str, object]) -> str:
        return (
            '{"customer_id":%d,"name":"%s","city":"%s","segment":"%s",'
            '"updated_at":"%s","payload":"%s"}'
            % (
                row["customer_id"],
                row["name"],
                row["city"],
                row["segment"],
                json_time(row["updated_at"]),
                row["payload"],
            )
        )


class Payments(TableSpec):
    def __init__(self) -> None:
        super().__init__(
            name="payments",
            columns=(
                "payment_id",
                "order_id",
                "paid_amount",
                "payment_time",
                "updated_at",
                "payload",
            ),
            primary_key="payment_id",
        )

    def row(self, index: int, total: int) -> Dict[str, object]:
        amount_cents = (index * 53) % 500_000
        payment_time = BASE_TIME + timedelta(seconds=index + 10)
        updated_at = payment_time + timedelta(seconds=(index * 19) % 43_200)
        return {
            "payment_id": index,
            "order_id": index,
            "amount_cents": amount_cents,
            "payment_time": payment_time,
            "updated_at": updated_at,
            "payload": payload(index + 13, 64),
        }

    def csv_values(self, row: Dict[str, object]) -> Sequence[object]:
        cents = int(row["amount_cents"])
        return (
            row["payment_id"],
            row["order_id"],
            f"{cents // 100}.{cents % 100:02d}",
            csv_time(row["payment_time"]),
            csv_time(row["updated_at"]),
            row["payload"],
        )

    def json_line(self, row: Dict[str, object]) -> str:
        cents = int(row["amount_cents"])
        return (
            '{"payment_id":%d,"order_id":%d,"paid_amount":%d.%02d,'
            '"payment_time":"%s","updated_at":"%s","payload":"%s"}'
            % (
                row["payment_id"],
                row["order_id"],
                cents // 100,
                cents % 100,
                json_time(row["payment_time"]),
                json_time(row["updated_at"]),
                row["payload"],
            )
        )


TABLES = {
    "orders": Orders(),
    "customers": Customers(),
    "payments": Payments(),
}


def display_path(path: Path) -> str:
    """Return a repository-relative path when possible, otherwise an absolute path."""
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path.resolve())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(path: Path, manifest: Dict[str, object]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    os.replace(temp, path)


def read_manifest(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def selected_tables(value: str) -> Sequence[TableSpec]:
    names = [item.strip() for item in value.split(",") if item.strip()]
    unknown = sorted(set(names) - set(TABLES))
    if unknown:
        raise SystemExit(f"unknown tables: {', '.join(unknown)}")
    return [TABLES[name] for name in names]


def progress(label: str, count: int, total: int, started: float) -> None:
    elapsed = max(time.time() - started, 1e-9)
    rate = count / elapsed
    print(
        f"[{label}] {count:,}/{total:,} rows "
        f"({count * 100.0 / total:5.1f}%, {rate:,.0f} rows/s)",
        flush=True,
    )


def iter_rows(spec: TableSpec, start: int, count: int, total: int) -> Iterator[Dict[str, object]]:
    for index in range(start, start + count):
        yield spec.row(index, total)
