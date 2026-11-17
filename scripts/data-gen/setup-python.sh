#!/usr/bin/env bash
#
# Create the local Python virtual environment used by the Mongo, Kafka and
# Elasticsearch loaders.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV="$ROOT/.venv"

python3 -m venv "$VENV"
"$VENV/bin/python" -m pip install --upgrade pip
"$VENV/bin/python" -m pip install -r "$SCRIPT_DIR/requirements.txt"
echo "python environment ready: $VENV/bin/python"
