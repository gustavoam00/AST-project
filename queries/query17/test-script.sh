#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PATH="$SCRIPT_DIR"
FILE_PATH="${TEST_CASE_LOCATION:-$DEFAULT_PATH}/query.sql"

python3 src/test_bug.py --query "$FILE_PATH" --oracle "DIFF" --notfull