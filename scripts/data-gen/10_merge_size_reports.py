#!/usr/bin/env python3
"""Merge per-system size reports collected while profiles ran one at a time."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = ROOT / "results" / "data-scale" / "size-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", help="per-system JSON reports")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--require-systems",
        default="csv,postgres,mysql,mongo,es,kafka,lakehouse",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    required = {item.strip() for item in args.require_systems.split(",") if item.strip()}
    merged: dict[str, object] = {"thresholdGiB": None, "tables": {}}
    for report_path in args.reports:
        report = json.loads(Path(report_path).read_text())
        if merged["thresholdGiB"] is None:
            merged["thresholdGiB"] = report.get("thresholdGiB")
            merged["thresholdBytes"] = report.get("thresholdBytes")
        for table, systems in report.get("tables", {}).items():
            merged["tables"].setdefault(table, {}).update(systems)

    failures = []
    missing = []
    for table, systems in sorted(merged["tables"].items()):
        for system, entry in sorted(systems.items()):
            if entry.get("status") == "failed":
                failures.append(f"{table}/{system}: {entry.get('gib')} GiB")
        present = set(systems)
        for system in sorted(required):
            # Kafka carries the orders stream only; customers/payments are not
            # produced as Kafka topics in the current test data set.
            if system == "kafka" and table != "orders":
                continue
            if system == "lakehouse":
                if not any(key.startswith("lakehouse.") for key in present):
                    missing.append(f"{table}/{system}")
            elif system not in present:
                missing.append(f"{table}/{system}")

    merged["requiredSystems"] = sorted(required)
    merged["failures"] = failures
    merged["missing"] = missing
    merged["passed"] = not failures and not missing
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"out": str(args.out), "failed": failures, "missing": missing}, indent=2))
    return 0 if merged["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
