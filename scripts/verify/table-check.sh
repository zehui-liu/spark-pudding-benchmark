#!/usr/bin/env bash
#
# Run paimon-check.py through spark-submit with the matching lakehouse runtime.
#
# Usage: scripts/verify/table-check.sh paimon /path/to/table --count 10000 ...
#        scripts/verify/table-check.sh iceberg /path/to/table --count 100 ...
#
# All arguments after the format are passed to paimon-check.py.

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

format="${1:-}"
if [[ -z "$format" ]]; then
  echo "usage: $0 <paimon|iceberg|hudi|parquet> <path> [checker args...]" >&2
  exit 2
fi
shift

case "$format" in
  paimon) runtime="$PUDDING_HOME/libs/paimon-runtime-1.0.0.jar" ;;
  iceberg) runtime="$PUDDING_HOME/libs/iceberg-runtime-1.0.0.jar" ;;
  hudi) runtime="$PUDDING_HOME/libs/hudi-runtime-1.0.0.jar" ;;
  parquet) runtime="" ;;
  *)
    echo "unsupported format: $format" >&2
    exit 2
    ;;
esac

if [[ -n "$runtime" && ! -f "$runtime" ]]; then
  echo "runtime jar not found: $runtime" >&2
  exit 1
fi

jar_arg=()
if [[ -n "$runtime" ]]; then
  jar_arg=(--jars "$runtime")
fi

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
"$SPARK_HOME/bin/spark-submit" --master 'local[2]' "${jar_arg[@]}" \
  "$SCRIPT_DIR/paimon-check.py" "$@" --format "$format"
