#!/bin/bash

SQL_FILE="$1"
ORACLE_TYPE="$2"

python3 src/test_bug.py --query "$SQL_FILE" --oracle "$ORACLE_TYPE"