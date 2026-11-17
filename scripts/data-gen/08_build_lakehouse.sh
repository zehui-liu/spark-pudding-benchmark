#!/usr/bin/env bash
#
# Build Hudi, Paimon or Iceberg baseline tables with the runtime jar shipped in
# the spark-pudding distribution.
#
# Usage:
#   scripts/data-gen/08_build_lakehouse.sh --format hudi
#   scripts/data-gen/08_build_lakehouse.sh --format paimon --tables orders
#   scripts/data-gen/08_build_lakehouse.sh --format iceberg --limit 1000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PUDDING_HOME="${PUDDING_HOME:-$ROOT/../spark-pudding/build/target/spark-pudding-1.0.0}"
SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"

# The interactive shell may still point at an x86 JDK 8. Prefer an arm64 JDK 11
# unless the operator explicitly selects another Java home.
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi
export PATH="$JAVA_HOME/bin:$PATH"

format=""
for argument in "$@"; do
  case "$argument" in
    hudi|paimon|iceberg)
      format="$argument"
      ;;
  esac
done

if [[ -z "$format" ]]; then
  echo "missing --format hudi|paimon|iceberg" >&2
  exit 2
fi

runtime_jar="$(ls "$PUDDING_HOME"/libs/${format}-runtime-*.jar 2>/dev/null | head -1 || true)"
if [[ -z "$runtime_jar" ]]; then
  echo "runtime jar not found: $PUDDING_HOME/libs/${format}-runtime-*.jar" >&2
  exit 1
fi

if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
  echo "spark-submit not found under SPARK_HOME: $SPARK_HOME" >&2
  exit 1
fi

echo "building $format baseline with $runtime_jar"
exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[${PUDDING_LOCAL_CORES:-4}]" \
  --driver-memory "${PUDDING_DRIVER_MEMORY:-4g}" \
  --jars "$runtime_jar" \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  "$SCRIPT_DIR/08_build_lakehouse.py" "$@"
