#!/bin/bash

# Test script demonstrating all output formats in DDB
# Shows TABLE, JSON, YAML, CSV, and XML output

echo "====================================="
echo "DDB Output Formats Test"
echo "====================================="
echo

# Build if needed
if [ ! -f "./target/release/ddb" ]; then
    echo "Building DDB..."
    cargo build --release
    echo
fi

DDB="./target/release/ddb"
QUERY="SELECT order_id, customer_name, product, price FROM sales_data WHERE region = 'West' LIMIT 2"

echo "Query: $QUERY"
echo
echo "====================================="

echo
echo "1. TABLE FORMAT (default)"
echo "-----------------------------------"
$DDB --config examples/config --query "$QUERY" --output table
echo

echo "2. JSON FORMAT"
echo "-----------------------------------"
$DDB --config examples/config --query "$QUERY" --output json
echo

echo "3. YAML FORMAT"
echo "-----------------------------------"
$DDB --config examples/config --query "$QUERY" --output yaml
echo

echo "4. CSV FORMAT"
echo "-----------------------------------"
$DDB --config examples/config --query "$QUERY" --output csv
echo

echo "5. XML FORMAT"
echo "-----------------------------------"
$DDB --config examples/config --query "$QUERY" --output xml
echo

echo "====================================="
echo "All 5 output formats demonstrated!"
echo "====================================="
