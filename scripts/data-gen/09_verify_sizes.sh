#!/usr/bin/env bash
#
# Verify the 1 GiB per-table scale requirement.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/09_verify_sizes.py" "$@"
