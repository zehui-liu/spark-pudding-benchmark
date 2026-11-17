#!/usr/bin/env bash
#
# Create an empty Paimon primary-key table for streaming CDC cases.
#
# Usage: scripts/run/create-paimon-table.sh <warehouse> <database> <table> [orders|mongo]
# The table path is <warehouse>/<database>.db/<table>.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "usage: $0 <warehouse> <database> <table> [orders|mongo]" >&2
  exit 2
fi

runtime="$PUDDING_HOME/libs/paimon-runtime-1.0.0.jar"
if [[ ! -f "$runtime" ]]; then
  echo "runtime jar not found: $runtime" >&2
  exit 1
fi

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --jars "$runtime" \
  "$SCRIPT_DIR/create-paimon-table.py" "$@"
