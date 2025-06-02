#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PATH="$SCRIPT_DIR/query.sql"
FILE_PATH="${TEST_CASE_LOCATION:-$DEFAULT_PATH}"

python3 src/test_bug.py --query "$FILE_PATH" --oracle "DIFF"