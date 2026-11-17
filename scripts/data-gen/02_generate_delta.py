#!/usr/bin/env python3
"""Generate the incremental change set applied by the sync/CDC test cases.

Outputs:
  delta_insert.csv  new orders (all columns)
  delta_update.csv  updated orders (all columns, updated_at newer than baseline)
  delta_delete.csv  order ids to delete
  delta_manifest.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common import (
    BASE_TIME,
    DEFAULT_OUT,
    ROOT,
    display_path,
    STATUSES,
    Orders,
    csv_time,
    write_manifest,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-rows", type=int, default=10_000_000)
    parser.add_argument("--insert", type=int, default=50_000)
    parser.add_argument("--update", type=int, default=200_000)
    parser.add_argument("--delete", type=int, default=20_000)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def pick_ids(rows: int, count: int, seed: int) -> list[int]:
    if count > rows:
        raise SystemExit(f"requested {count} ids from a table with only {rows} rows")
    rng = random.Random(seed)
    return rng.sample(range(1, rows + 1), count)


def write_rows(path: Path, spec: Orders, rows: list[dict[str, object]]) -> int:
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(spec.columns)
        for row in rows:
            writer.writerow(spec.csv_values(row))
    os.replace(temp, path)
    return path.stat().st_size


def main() -> int:
    args = parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    spec = Orders()
    started = time.time()

    update_ids = pick_ids(args.base_rows, args.update, args.seed)
    delete_ids = pick_ids(args.base_rows, args.delete, args.seed + 1)
    update_set = set(update_ids)
    delete_ids = [item for item in delete_ids if item not in update_set]
    delete_set = set(delete_ids)
    if len(delete_ids) < args.delete:
        extra = pick_ids(args.base_rows, args.delete * 2, args.seed + 2)
        for item in extra:
            if len(delete_ids) >= args.delete:
                break
            if item not in update_set and item not in delete_set:
                delete_ids.append(item)
                delete_set.add(item)

    delta_time = BASE_TIME + timedelta(seconds=args.base_rows + 2 * 86_400)

    insert_rows = []
    for offset in range(1, args.insert + 1):
        order_id = args.base_rows + offset
        insert_rows.append(spec.row(order_id, args.base_rows + args.insert))

    update_rows = []
    for offset, order_id in enumerate(update_ids):
        row = spec.row(order_id, args.base_rows)
        cents = (order_id * 97 + offset) % 1_000_000
        row["amount_cents"] = cents
        row["status"] = STATUSES[(offset + 1) % len(STATUSES)]
        row["updated_at"] = delta_time + timedelta(seconds=offset)
        update_rows.append(row)

    files = {
        "insert": out / "delta_insert.csv",
        "update": out / "delta_update.csv",
        "delete": out / "delta_delete.csv",
    }
    if any(path.exists() for path in files.values()) and not args.force:
        print("delta files already exist, skipping (use --force to rebuild)", flush=True)
        return 0

    insert_bytes = write_rows(files["insert"], spec, insert_rows)
    update_bytes = write_rows(files["update"], spec, update_rows)

    temp = files["delete"].with_suffix(files["delete"].suffix + ".tmp")
    with temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["order_id"])
        for order_id in delete_ids:
            writer.writerow([order_id])
    os.replace(temp, files["delete"])
    delete_bytes = files["delete"].stat().st_size

    manifest = {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "baseRows": args.base_rows,
        "deltaTime": csv_time(delta_time),
        "counts": {
            "insert": len(insert_rows),
            "update": len(update_rows),
            "delete": len(delete_ids),
        },
        "files": {
            "insert": {"path": display_path(files["insert"]), "bytes": insert_bytes},
            "update": {"path": display_path(files["update"]), "bytes": update_bytes},
            "delete": {"path": display_path(files["delete"]), "bytes": delete_bytes},
        },
    }
    write_manifest(out / "delta_manifest.json", manifest)
    elapsed = time.time() - started
    print(
        f"delta: insert={len(insert_rows):,} update={len(update_rows):,} "
        f"delete={len(delete_ids):,} in {elapsed:.1f}s",
        flush=True,
    )
    print(f"manifest: {out / 'delta_manifest.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
