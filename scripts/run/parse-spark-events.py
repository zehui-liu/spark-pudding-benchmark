#!/usr/bin/env python3
"""Summarize Spark event logs and benchmark resource samples.

Usage:
  parse-spark-events.py <bench-dir> <case-id> <exit-code> <wall-seconds> [rows]
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Iterable


def iter_events(root: Path) -> Iterable[dict[str, Any]]:
    if not root.exists():
        return
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(event, dict) and "Event" in event:
                        yield event
        except OSError:
            continue


def number(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return 0.0


def parse_bytes(text: str) -> float:
    units = [
        ("TiB", 1024 ** 4),
        ("GiB", 1024 ** 3),
        ("MiB", 1024 ** 2),
        ("KiB", 1024),
        ("B", 1),
    ]
    text = text.strip()
    for unit, factor in units:
        if text.endswith(unit):
            try:
                return float(text[: -len(unit)].strip()) * factor
            except ValueError:
                return 0.0
    return 0.0


def parse_rss(path: Path) -> dict[str, float]:
    peak = 0.0
    if not path.exists():
        return {"peakDriverRssBytes": peak}
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        next(handle, None)
        for line in handle:
            parts = line.strip().split(",")
            if len(parts) != 2:
                continue
            try:
                peak = max(peak, float(parts[1]) * 1024.0)
            except ValueError:
                continue
    return {"peakDriverRssBytes": peak}


def parse_docker(path: Path) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    if not path.exists():
        return result
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        next(handle, None)
        for line in handle:
            parts = line.rstrip("\n").split(",")
            if len(parts) < 4:
                continue
            name, usage, cpu = parts[1], parts[2], parts[3]
            mem_text = usage.split("/")[0].strip()
            try:
                cpu_value = float(cpu.rstrip("%").strip())
            except ValueError:
                cpu_value = 0.0
            entry = result.setdefault(
                name, {"peakMemoryBytes": 0.0, "peakCpuPercent": 0.0}
            )
            entry["peakMemoryBytes"] = max(entry["peakMemoryBytes"], parse_bytes(mem_text))
            entry["peakCpuPercent"] = max(entry["peakCpuPercent"], cpu_value)
    return result


def main() -> int:
    if len(sys.argv) < 5:
        print(__doc__, file=sys.stderr)
        return 2
    bench_dir = Path(sys.argv[1])
    case_id = sys.argv[2]
    exit_code = int(sys.argv[3])
    wall_seconds = float(sys.argv[4])
    rows = float(sys.argv[5]) if len(sys.argv) > 5 else 0.0

    event_root = bench_dir / "events"
    tasks = 0
    failed_tasks = 0
    killed_tasks = 0
    executor_run_ms = 0.0
    deserialize_ms = 0.0
    result_serialize_ms = 0.0
    memory_spilled = 0.0
    disk_spilled = 0.0
    shuffle_read = 0.0
    shuffle_write = 0.0
    input_bytes = 0.0
    sql_seconds = 0.0
    sql_count = 0
    sql_start: dict[str, float] = {}
    app_seconds = 0.0
    current_app_start: float | None = None
    job_start: dict[str, float] = {}
    job_durations: list[float] = []

    for event in iter_events(event_root):
        kind = event.get("Event", "")
        if kind == "SparkListenerApplicationStart":
            current_app_start = number(event.get("Timestamp"))
        elif kind == "SparkListenerApplicationEnd":
            ended = number(event.get("Timestamp"))
            if current_app_start is not None and ended >= current_app_start:
                app_seconds += (ended - current_app_start) / 1000.0
            current_app_start = None
        elif kind == "SparkListenerTaskEnd":
            tasks += 1
            info = event.get("Task Info") or {}
            if info.get("Failed"):
                failed_tasks += 1
            if info.get("Killed"):
                killed_tasks += 1
            metrics = event.get("Task Metrics") or {}
            executor_run_ms += number(metrics.get("Executor Run Time"))
            deserialize_ms += number(metrics.get("Executor Deserialize Time"))
            result_serialize_ms += number(metrics.get("Result Serialization Time"))
            memory_spilled += number(metrics.get("Memory Bytes Spilled"))
            disk_spilled += number(metrics.get("Disk Bytes Spilled"))
            shuffle_read_metrics = metrics.get("Shuffle Read Metrics") or {}
            shuffle_write_metrics = metrics.get("Shuffle Write Metrics") or {}
            input_metrics = metrics.get("Input Metrics") or {}
            shuffle_read += number(shuffle_read_metrics.get("Total Bytes Read"))
            shuffle_write += number(shuffle_write_metrics.get("Bytes Written"))
            input_bytes += number(input_metrics.get("Bytes Read"))
        elif kind.endswith("SparkListenerSQLExecutionStart"):
            execution_id = str(event.get("executionId", event.get("Execution ID", "")))
            sql_start[execution_id] = number(event.get("time", event.get("Time", 0)))
        elif kind.endswith("SparkListenerSQLExecutionEnd"):
            execution_id = str(event.get("executionId", event.get("Execution ID", "")))
            started = sql_start.pop(execution_id, 0.0)
            ended = number(event.get("time", event.get("Time", 0)))
            if ended >= started > 0:
                sql_seconds += (ended - started) / 1000.0
            sql_count += 1
        elif kind == "SparkListenerJobStart":
            job_id = str(event.get("Job ID", event.get("Job Id", "")))
            job_start[job_id] = number(event.get("Submission Time"))
        elif kind == "SparkListenerJobEnd":
            job_id = str(event.get("Job ID", event.get("Job Id", "")))
            started = job_start.pop(job_id, 0.0)
            completed = number(event.get("Completion Time"))
            if started > 0 and completed >= started:
                job_durations.append((completed - started) / 1000.0)

    resources = parse_rss(bench_dir / "rss.csv")
    docker = parse_docker(bench_dir / "docker.csv")
    result = {
        "caseId": case_id,
        "exitCode": exit_code,
        "rows": rows,
        "wallSeconds": wall_seconds,
        "rowsPerSecond": (rows / wall_seconds) if wall_seconds > 0 else 0.0,
        "inputBytes": input_bytes,
        "inputMiB": input_bytes / (1024.0 ** 2),
        "inputMiBPerSecond": (
            input_bytes / (1024.0 ** 2) / wall_seconds if wall_seconds > 0 else 0.0
        ),
        "sparkApplicationSeconds": app_seconds,
        "sqlExecutionSeconds": sql_seconds,
        "sqlExecutionCount": sql_count,
        "jobCount": len(job_durations),
        "jobSecondsTotal": sum(job_durations),
        "jobSecondsMax": max(job_durations) if job_durations else 0.0,
        "tasks": tasks,
        "failedTasks": failed_tasks,
        "killedTasks": killed_tasks,
        "executorRunSeconds": executor_run_ms / 1000.0,
        "executorDeserializeSeconds": deserialize_ms / 1000.0,
        "resultSerializeSeconds": result_serialize_ms / 1000.0,
        "memoryBytesSpilled": memory_spilled,
        "diskBytesSpilled": disk_spilled,
        "shuffleReadBytes": shuffle_read,
        "shuffleWriteBytes": shuffle_write,
        "peakDriverRssBytes": resources["peakDriverRssBytes"],
        "docker": docker,
    }
    output = bench_dir / "metrics.json"
    with output.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
