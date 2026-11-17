#!/usr/bin/env python3
"""Generate the deterministic CSV/JSONL datasets used by spark-pudding-benchmark.

The scale profile writes 10,000,000 rows per table, which is at least 1 GiB of
uncompressed logical data for orders, customers and payments. The smoke profile
writes 100,000 rows per table for fast iteration.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from common import (
    DEFAULT_OUT,
    ROOT,
    display_path,
    TableSpec,
    progress,
    read_manifest,
    selected_tables,
    sha256_file,
    write_manifest,
)

PROFILE_ROWS = {
    "smoke": 100_000,
    "scale": 10_000_000,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tables", default="orders,customers,payments")
    parser.add_argument("--profile", choices=sorted(PROFILE_ROWS), default="scale")
    parser.add_argument("--rows", type=int, default=None, help="rows per table; overrides --profile")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--formats", default="csv,jsonl")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--force", action="store_true", help="overwrite existing files")
    parser.add_argument("--no-sha256", action="store_true", help="skip file hashing")
    return parser.parse_args()


def render_formats(value: str) -> list[str]:
    formats = [item.strip().lower() for item in value.split(",") if item.strip()]
    unknown = sorted(set(formats) - {"csv", "jsonl"})
    if unknown:
        raise SystemExit(f"unknown formats: {', '.join(unknown)}")
    return formats


def expected_files(spec: TableSpec, out: Path, formats: list[str]) -> dict[str, Path]:
    suffixes = {"csv": ".csv", "jsonl": ".jsonl"}
    return {fmt: out / f"{spec.name}{suffixes[fmt]}" for fmt in formats}


def write_table(
    spec: TableSpec,
    rows: int,
    out: Path,
    formats: list[str],
    force: bool,
    digest: bool,
) -> dict[str, object]:
    files = expected_files(spec, out, formats)
    if not force and all(path.exists() and path.stat().st_size > 0 for path in files.values()):
        print(f"[{spec.name}] existing files found, skipping (use --force to rebuild)", flush=True)
        return {
            "rows": rows,
            "files": {
                fmt: {
                    "path": display_path(path),
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path) if digest else None,
                }
                for fmt, path in files.items()
            },
            "skipped": True,
        }

    out.mkdir(parents=True, exist_ok=True)
    temporary = {fmt: path.with_suffix(path.suffix + ".tmp") for fmt, path in files.items()}
    started = time.time()
    csv_handle = temporary["csv"].open("w", newline="", encoding="utf-8") if "csv" in formats else None
    json_handle = temporary["jsonl"].open("w", encoding="utf-8") if "jsonl" in formats else None
    try:
        writer = csv.writer(csv_handle, lineterminator="\n") if csv_handle else None
        if writer:
            writer.writerow(spec.columns)
        for index in range(1, rows + 1):
            row = spec.row(index, rows)
            if writer:
                writer.writerow(spec.csv_values(row))
            if json_handle:
                json_handle.write(spec.json_line(row) + "\n")
            if index % 1_000_000 == 0 or index == rows:
                progress(spec.name, index, rows, started)
    finally:
        if csv_handle:
            csv_handle.close()
        if json_handle:
            json_handle.close()

    for fmt, path in files.items():
        os.replace(temporary[fmt], path)

    result = {
        "rows": rows,
        "files": {
            fmt: {
                "path": display_path(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path) if digest else None,
            }
            for fmt, path in files.items()
        },
        "skipped": False,
    }
    for fmt, info in result["files"].items():
        size_gib = info["bytes"] / (1024 ** 3)
        print(
            f"[{spec.name}] {fmt}: {info['path']} "
            f"{info['bytes']:,} bytes ({size_gib:.2f} GiB)",
            flush=True,
        )
        if size_gib < 1.0:
            print(
                f"[{spec.name}] WARNING: {fmt} is below 1 GiB; increase --rows "
                "before using this dataset for the scale gate",
                file=sys.stderr,
            )
    return result


def main() -> int:
    args = parse_args()
    rows = args.rows if args.rows is not None else PROFILE_ROWS[args.profile]
    if rows <= 0:
        raise SystemExit("--rows must be positive")
    formats = render_formats(args.formats)
    if not formats:
        raise SystemExit("at least one format is required")

    out = args.out.resolve()
    manifest_path = out / "manifest.json"
    manifest = read_manifest(manifest_path)
    tables = selected_tables(args.tables)

    print(
        f"generating profile={args.profile} rows={rows:,} tables={args.tables} "
        f"formats={','.join(formats)} out={out}",
        flush=True,
    )
    generated = {}
    for spec in tables:
        generated[spec.name] = write_table(
            spec=spec,
            rows=rows,
            out=out,
            formats=formats,
            force=args.force,
            digest=not args.no_sha256,
        )

    manifest.update(
        {
            "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "profile": args.profile,
            "rowsPerTable": rows,
            "seed": args.seed,
            "formats": formats,
        }
    )
    manifest.setdefault("tables", {}).update(generated)
    write_manifest(manifest_path, manifest)
    print(f"manifest: {manifest_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
