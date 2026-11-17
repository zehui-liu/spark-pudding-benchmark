#!/usr/bin/env python3
"""Append JSON lines from a file to an existing Kafka topic.

Usage: produce-kafka-json.py --bootstrap localhost:19092 --topic orders-smoke \
         --file /path/to/records.jsonl
"""

from __future__ import annotations

import argparse

from confluent_kafka import Producer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})
    delivered = 0

    def report(error, _message) -> None:
        nonlocal delivered
        if error is not None:
            raise RuntimeError(f"kafka delivery failed: {error}")
        delivered += 1

    with open(args.file, "r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            producer.produce(args.topic, text.encode("utf-8"), callback=report)
            producer.poll(0)
    producer.flush()
    print(f"produced {delivered} records to {args.topic}")


if __name__ == "__main__":
    main()
