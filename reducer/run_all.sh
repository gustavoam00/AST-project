#!/bin/bash

for i in {1..20}; do
  echo "Running reducer for query$i..."
  ./reducer --query queries/query$i/original_test.sql --test queries/query$i/test-script.sh
  echo ""
done