#!/usr/bin/env python3
"""Render a markdown summary for results/bench/*/metrics.json."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def fmt_bytes(value: float) -> str:
    if value >= 1024 ** 3:
        return f"{value / (1024 ** 3):.2f} GiB"
    if value >= 1024 ** 2:
        return f"{value / (1024 ** 2):.1f} MiB"
    if value >= 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value:.0f} B"


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("results/bench")
    rows = []
    for metrics_path in sorted(root.glob("*/metrics.json")):
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        meta = {}
        meta_path = metrics_path.parent / "meta.env"
        if meta_path.exists():
            for line in meta_path.read_text(encoding="utf-8").splitlines():
                if "=" in line:
                    key, value = line.split("=", 1)
                    meta[key] = value
        rows.append((metrics_path.parent.name, metrics, meta))

    print("# Benchmark summary")
    print()
    print(
        "| case | exit | wall s | rows | rows/s | input MiB/s | "
        "spark s | peak RSS | spill | shuffle read/write |"
    )
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
    for name, metrics, _ in rows:
        spill = float(metrics.get("memoryBytesSpilled", 0)) + float(
            metrics.get("diskBytesSpilled", 0)
        )
        print(
            "| {case} | {exit} | {wall:.0f} | {rows:.0f} | {rps:.1f} | "
            "{mbps:.2f} | {spark:.1f} | {rss} | {spill} | {read}/{write} |".format(
                case=name,
                exit=metrics.get("exitCode", ""),
                wall=float(metrics.get("wallSeconds", 0)),
                rows=float(metrics.get("rows", 0)),
                rps=float(metrics.get("rowsPerSecond", 0)),
                mbps=float(metrics.get("inputMiBPerSecond", 0)),
                spark=float(metrics.get("sparkApplicationSeconds", 0)),
                rss=fmt_bytes(float(metrics.get("peakDriverRssBytes", 0))),
                spill=fmt_bytes(spill),
                read=fmt_bytes(float(metrics.get("shuffleReadBytes", 0))),
                write=fmt_bytes(float(metrics.get("shuffleWriteBytes", 0))),
            )
        )
    if not rows:
        print("No benchmark results found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
