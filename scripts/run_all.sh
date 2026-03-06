#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Step 1: rearrange_rawdata.py ==="
python "$SCRIPT_DIR/rearrange_rawdata.py"

echo "=== Step 2: rearrange_dataset.py ==="
python "$SCRIPT_DIR/rearrange_dataset.py"

echo "=== Done ==="
