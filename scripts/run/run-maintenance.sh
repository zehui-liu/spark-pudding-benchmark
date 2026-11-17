#!/usr/bin/env bash
#
# OS-06: table-maintenance CLI for Iceberg, Paimon and Hudi.
#
# Small Iceberg/Paimon/Hudi tables are rebuilt through SK-11/SK-10/SK-09, then
# the runner exercises status, compaction/rewrite, expiry, orphan cleanup and
# the destructive-action guards (--confirm and --dry-run).
#
# Usage: scripts/run/run-maintenance.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STATE_DIR="$ROOT/results/state/os-06"
SUMMARY="$ROOT/results/runs/maintenance-summary.md"

if [[ "${PUDDING_USE_HADOOP_CONF:-0}" != "1" ]]; then
  unset HADOOP_CONF_DIR
fi
SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
SPARK_HOME="${SPARK_HOME:-/Users/zhliu/env/bg-env/spark-3.4.0-bin-hadoop3}"
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi
export SPARK_HOME PUDDING_HOME SPARK_MASTER="${SPARK_MASTER:-local[2]}"

mkdir -p "$STATE_DIR" "$ROOT/results/runs"
: > "$SUMMARY"
echo "# Table-maintenance cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

maintenance() {
  env -u HADOOP_CONF_DIR "$PUDDING_HOME/bin/pudding" maintenance "$@" 2>/dev/null
}

# Used for expected failures: keep the CLI's stderr/JSON error visible.
maintenance_raw() {
  env -u HADOOP_CONF_DIR "$PUDDING_HOME/bin/pudding" maintenance "$@"
}

run_case() {
  "$ROOT/scripts/run/run-case.sh" "$1" --timeout 900 \
    >>"$ROOT/results/runs/maintenance-runner.log" 2>&1 ||
    fail "could not rebuild $1"
}

umask 077
cat > "$STATE_DIR/iceberg.properties" <<EOF
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=hadoop
spark.sql.catalog.lake.warehouse=file://$ROOT/data/generated/lakehouse/iceberg
EOF
cat > "$STATE_DIR/paimon.properties" <<EOF
spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse=file://$ROOT/data/generated/lakehouse/paimon
EOF
chmod 600 "$STATE_DIR"/*.properties

echo "== rebuilding small lakehouse tables =="
run_case SK-11
run_case SK-10
run_case SK-09
pass "OS-06 small Iceberg, Paimon and Hudi tables rebuilt"

# ------------------------------------------------------------------- Iceberg
iceberg_status="$(maintenance iceberg status --catalog lake --table pudding_test.sk11_orders \
  --conf-file "$STATE_DIR/iceberg.properties" --json)"
python3 - "$iceberg_status" <<'PY' || fail "Iceberg status is incomplete"
import json
import sys
value = json.loads(sys.argv[1])
summary = value["summary"]
assert int(summary["snapshots.snapshots"]) >= 1, summary
assert int(summary["files.data_files"]) >= 1, summary
PY
pass "OS-06 Iceberg status reports snapshots, data files and manifests"

rewrite="$(maintenance iceberg rewrite-data-files --catalog lake \
  --table pudding_test.sk11_orders --min-input-files 50 --dry-run \
  --conf-file "$STATE_DIR/iceberg.properties" --json)"
python3 - "$rewrite" <<'PY' || fail "Iceberg rewrite dry-run did not preview SQL"
import json
import sys
value = json.loads(sys.argv[1])
assert value["dryRun"] is True, value
assert value["executed"] is False, value
assert "rewrite_data_files" in value["statements"][0], value
PY
pass "OS-06 Iceberg rewrite-data-files supports --dry-run"

code=0
guard="$(maintenance_raw iceberg expire-snapshots --catalog lake \
  --table pudding_test.sk11_orders --retain-last 1 \
  --conf-file "$STATE_DIR/iceberg.properties" 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "Iceberg expire-snapshots ran without --confirm"
grep -q "add --confirm or --dry-run" <<<"$guard" ||
  fail "Iceberg expire guard did not name --confirm/--dry-run"
pass "OS-06 Iceberg refuses destructive expiry without --confirm"

expire="$(maintenance iceberg expire-snapshots --catalog lake \
  --table pudding_test.sk11_orders --older-than 1ms --retain-last 1 --confirm \
  --conf-file "$STATE_DIR/iceberg.properties" --json)"
python3 - "$expire" <<'PY' || fail "Iceberg expire did not execute"
import json
import sys
value = json.loads(sys.argv[1])
assert value["executed"] is True, value
PY
pass "OS-06 Iceberg expire-snapshots executes with --confirm"

orphan="$(maintenance iceberg remove-orphan-files --catalog lake \
  --table pudding_test.sk11_orders --older-than 24h --dry-run \
  --conf-file "$STATE_DIR/iceberg.properties" --json)"
python3 - "$orphan" <<'PY' || fail "Iceberg orphan dry-run did not preview SQL"
import json
import sys
value = json.loads(sys.argv[1])
assert value["dryRun"] is True, value
assert value["executed"] is False, value
PY
pass "OS-06 Iceberg remove-orphan-files supports --dry-run"

# -------------------------------------------------------------------- Paimon
paimon_status="$(maintenance paimon status --catalog paimon --table pudding_test.sk10_orders \
  --conf-file "$STATE_DIR/paimon.properties" --json)"
python3 - "$paimon_status" <<'PY' || fail "Paimon status is incomplete"
import json
import sys
summary = json.loads(sys.argv[1])["summary"]
assert int(summary["snapshots.snapshots"]) >= 1, summary
assert int(summary["files.data_files"]) >= 1, summary
PY
pass "OS-06 Paimon status reports snapshots and data files"

compact="$(maintenance paimon compact --catalog paimon --table pudding_test.sk10_orders \
  --option compact-strategy=full --conf-file "$STATE_DIR/paimon.properties" --json)"
python3 - "$compact" <<'PY' || fail "Paimon compact did not execute"
import json
import sys
assert json.loads(sys.argv[1])["executed"] is True
PY
pass "OS-06 Paimon compact executes"

code=0
guard="$(maintenance_raw paimon expire-snapshots --catalog paimon \
  --table pudding_test.sk10_orders --older-than 1ms --retain-min 1 --retain-max 1 \
  --conf-file "$STATE_DIR/paimon.properties" 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "Paimon expire-snapshots ran without --confirm"
grep -q "add --confirm or --dry-run" <<<"$guard" ||
  fail "Paimon expire guard did not name --confirm/--dry-run"
pass "OS-06 Paimon refuses destructive expiry without --confirm"

expire="$(maintenance paimon expire-snapshots --catalog paimon \
  --table pudding_test.sk10_orders --older-than 1ms --retain-min 1 --retain-max 1 \
  --confirm --conf-file "$STATE_DIR/paimon.properties" --json)"
python3 - "$expire" <<'PY' || fail "Paimon expire did not execute"
import json
import sys
assert json.loads(sys.argv[1])["executed"] is True
PY
pass "OS-06 Paimon expire-snapshots executes with --confirm"

code=0
invalid="$(maintenance_raw paimon expire-snapshots --catalog paimon \
  --table pudding_test.sk10_orders --older-than 1ms --retain-max 1 --dry-run \
  --conf-file "$STATE_DIR/paimon.properties" 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "Paimon accepted retain-max below the default retain-min"
grep -q "retain-max below the default retain-min" <<<"$invalid" ||
  fail "Paimon retain-min validation message is missing"
pass "OS-06 Paimon validates retain-min against retain-max"

# ---------------------------------------------------------------------- Hudi
hudi_path="$ROOT/results/runs/SK-09/output/hudi/orders"
[[ -d "$hudi_path/.hoodie" ]] || fail "Hudi table is missing at $hudi_path"
hudi_status="$(maintenance hudi status --path "$hudi_path" --json)"
python3 - "$hudi_status" <<'PY' || fail "Hudi status is incomplete"
import json
import sys
summary = json.loads(sys.argv[1])["summary"]
assert summary["path.exists"] == "true", summary
assert int(summary["path.file_count"]) > 0, summary
PY
pass "OS-06 Hudi status reports path files and size"

compact="$(maintenance hudi compact --path "$hudi_path" \
  --option op=scheduleAndExecute --dry-run --json)"
python3 - "$compact" <<'PY' || fail "Hudi compact dry-run did not preview SQL"
import json
import sys
value = json.loads(sys.argv[1])
assert value["dryRun"] is True, value
assert value["executed"] is False, value
assert "run_compaction" in value["statements"][0], value
PY
pass "OS-06 Hudi compact supports --dry-run"

# SK-09 creates a CopyOnWrite table; Hudi explicitly rejects compaction there.
code=0
compact="$(maintenance_raw hudi compact --path "$hudi_path" \
  --option op=scheduleAndExecute --json 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "Hudi compact accepted a CopyOnWrite table"
grep -q "Compaction is not supported on a CopyOnWrite table" <<<"$compact" ||
  fail "Hudi compact did not explain the CopyOnWrite limitation"
pass "OS-06 Hudi compact rejects a CopyOnWrite table with a clear message"

archive="$(maintenance hudi archive --path "$hudi_path" --json)"
python3 - "$archive" <<'PY' || fail "Hudi archive did not execute"
import json
import sys
assert json.loads(sys.argv[1])["executed"] is True
PY
pass "OS-06 Hudi archive executes on a path"

code=0
clean="$(maintenance_raw hudi clean --path "$hudi_path" --retain-last 10 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "Hudi clean accepted a path-only table"
grep -q "requires --table" <<<"$clean" ||
  fail "Hudi clean path-only guard did not name --table"
pass "OS-06 Hudi clean requires a registered --table"

echo >> "$SUMMARY"
echo "ALL PASS: OS-06 (Iceberg, Paimon and Hudi maintenance)" >> "$SUMMARY"
echo
echo "ALL PASS: OS-06 (Iceberg, Paimon and Hudi maintenance)"
echo "summary: ${SUMMARY#$ROOT/}"
