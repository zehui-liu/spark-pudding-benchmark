#!/usr/bin/env bash
#
# OS-08: capability manifest verify/show/diff.
#
# Verifies the built distribution, prints its manifest, diffs it against a
# modified copy (one removed component and one changed capability), and checks
# that missing jars or a missing root manifest are rejected.
#
# Usage: scripts/run/run-capabilities.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STATE_DIR="$ROOT/results/state/os-08"
SUMMARY="$ROOT/results/runs/capabilities-summary.md"

SIBLING_ROOT="$(cd "$ROOT/.." && pwd)"
PUDDING_HOME="${PUDDING_HOME:-$SIBLING_ROOT/spark-pudding/build/target/spark-pudding-1.0.0}"
DEFAULT_JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
if [[ -x "${PUDDING_JAVA_HOME:-}/bin/java" ]]; then
  export JAVA_HOME="$PUDDING_JAVA_HOME"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/java" ]]; then
  export JAVA_HOME="$DEFAULT_JAVA_HOME"
fi

export PUDDING_HOME

rm -rf "$STATE_DIR"
mkdir -p "$STATE_DIR" "$ROOT/results/runs"
: > "$SUMMARY"
echo "# Capability manifest cases ($(date -u +%Y-%m-%dT%H:%M:%SZ))" >> "$SUMMARY"
echo >> "$SUMMARY"

fail() {
  echo "FAIL: $*" | tee -a "$SUMMARY" >&2
  exit 1
}

[[ -f "$PUDDING_HOME/capabilities.yaml" ]] ||
  fail "capabilities.yaml is missing under $PUDDING_HOME"

pass() {
  echo "PASS: $*" | tee -a "$SUMMARY"
}

capabilities() {
  "$PUDDING_HOME/bin/pudding" capabilities "$@"
}

verify_one="$(capabilities verify "$PUDDING_HOME")" ||
  fail "capabilities verify failed on the built distribution"
verify_two="$(capabilities verify "$PUDDING_HOME")"
[[ "$verify_one" == "$verify_two" ]] || fail "verify output is not deterministic"
grep -q "capability manifest verified" <<<"$verify_one" ||
  fail "verify did not confirm the manifest"
python3 - "$verify_one" <<'PY' || fail "verify counts are incomplete"
import re
import sys
text = sys.argv[1]
counts = dict(re.findall(r"^\s+(catalog|connection|sink|source|transform): (\d+)$",
                         text, re.MULTILINE))
for kind in ("catalog", "connection", "sink", "source", "transform"):
    if int(counts.get(kind, 0)) <= 0:
        raise SystemExit(f"missing {kind} count: {counts}")
PY
pass "OS-08 verify accepts the distribution and reports component counts"

capabilities show "$PUDDING_HOME" > "$STATE_DIR/show.yaml" ||
  fail "capabilities show failed"
python3 - "$STATE_DIR/show.yaml" <<'PY' || fail "show output is not a complete manifest"
import sys
import yaml
value = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
assert value["schemaVersion"], value.keys()
assert value["productVersion"], value.keys()
assert len(value["components"]) > 0, value
keys = {component["kind"] + ":" + component["type"] for component in value["components"]}
for expected in ("source:kafka-source", "sink:iceberg-sink", "transform:filter-transform"):
    assert expected in keys, (expected, sorted(keys)[:10])
PY
pass "OS-08 show emits the merged manifest"

self_diff="$(capabilities diff "$PUDDING_HOME" "$PUDDING_HOME")" ||
  fail "self diff failed"
[[ "$self_diff" == "no capability changes" ]] || fail "self diff reported changes: $self_diff"
pass "OS-08 diff of the same distribution reports no changes"

mkdir -p "$STATE_DIR/modified"
python3 - "$PUDDING_HOME/capabilities.yaml" "$STATE_DIR/modified/capabilities.yaml" <<'PY'
import sys

import yaml

value = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
value["components"] = [
    component for component in value["components"]
    if not (component.get("kind") == "source" and component.get("type") == "iceberg-source")
]
for component in value["components"]:
    if component.get("kind") == "source" and component.get("type") == "kafka-source":
        component["capabilities"]["executionModes"] = ["batch"]
yaml.safe_dump(value, open(sys.argv[2], "w", encoding="utf-8"), sort_keys=False)
PY

diff="$(capabilities diff "$PUDDING_HOME" "$STATE_DIR/modified")" ||
  fail "diff against the modified manifest failed"
grep -q "removed:" <<<"$diff" || fail "diff did not report a removed component"
grep -q "source:iceberg-source" <<<"$diff" || fail "diff missed the removed source"
grep -q "changed:" <<<"$diff" || fail "diff did not report a changed component"
grep -q "source:kafka-source" <<<"$diff" || fail "diff missed the changed source"
pass "OS-08 diff reports removed and changed capabilities"

missing_dist="$STATE_DIR/missing-jar-dist"
mkdir -p "$missing_dist/libs"
ln -s "$PUDDING_HOME/capabilities.yaml" "$missing_dist/capabilities.yaml"
for jar in "$PUDDING_HOME"/libs/*.jar; do
  name="$(basename "$jar")"
  [[ "$name" == iceberg-sink-* ]] && continue
  ln -s "$jar" "$missing_dist/libs/$name"
done
code=0
missing="$(capabilities verify "$missing_dist" 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "verify accepted a distribution with a missing jar"
grep -q "references missing jars" <<<"$missing" ||
  fail "missing-jar verification did not name the missing jar"
grep -q "iceberg-sink" <<<"$missing" ||
  fail "missing-jar verification did not identify iceberg-sink"
pass "OS-08 verify rejects a distribution with a missing component jar"

mkdir -p "$STATE_DIR/empty-dist"
code=0
missing_root="$(capabilities verify "$STATE_DIR/empty-dist" 2>&1)" || code=$?
[[ "$code" -ne 0 ]] || fail "verify accepted a distribution without capabilities.yaml"
grep -q "capability manifest is missing" <<<"$missing_root" ||
  fail "missing-manifest verification did not explain the problem"
pass "OS-08 verify rejects a distribution without a root manifest"

echo >> "$SUMMARY"
echo "ALL PASS: OS-08 (capabilities verify/show/diff)" >> "$SUMMARY"
echo
echo "ALL PASS: OS-08 (capabilities verify/show/diff)"
echo "summary: ${SUMMARY#$ROOT/}"
