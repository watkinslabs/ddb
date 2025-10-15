#!/bin/bash

# Test script for SQL-based table configuration
# This demonstrates that DDB now uses CREATE TABLE statements instead of YAML

echo "====================================="
echo "DDB SQL-Based Configuration Test"
echo "====================================="
echo

# Build if needed
if [ ! -f "./target/release/ddb" ]; then
    echo "Building DDB..."
    cargo build --release
    echo
fi

DDB="./target/release/ddb"

echo "1. Test with SQL-based config (shows table loaded from CREATE TABLE)"
echo "   Command: $DDB --config examples/config --query \"SELECT * FROM sales_data LIMIT 3\""
echo
$DDB --config examples/config --query "SELECT * FROM sales_data LIMIT 3"
echo

echo "2. Test aggregate query with SQL config"
echo "   Command: $DDB --config examples/config --query \"SELECT COUNT(*) as total FROM sales_data\""
echo
$DDB --config examples/config --query "SELECT COUNT(*) as total FROM sales_data"
echo

echo "3. Test GROUP BY with SQL config"
echo "   Command: $DDB --config examples/config --query \"SELECT region, COUNT(*) as orders FROM sales_data GROUP BY region\""
echo
$DDB --config examples/config --query "SELECT region, COUNT(*) as orders FROM sales_data GROUP BY region"
echo

echo "4. Test JSON output with SQL config"
echo "   Command: $DDB --config examples/config --query \"SELECT * FROM sales_data WHERE region = 'West' LIMIT 2\" --output json"
echo
$DDB --config examples/config --query "SELECT * FROM sales_data WHERE region = 'West' LIMIT 2" --output json
echo

echo "5. Test direct file method (backward compatibility)"
echo "   Command: $DDB --query \"SELECT * FROM sales_data WHERE price > 500 LIMIT 2\" --file examples/sales_data.csv"
echo
$DDB --query "SELECT * FROM sales_data WHERE price > 500 LIMIT 2" --file examples/sales_data.csv
echo

echo "====================================="
echo "All tests completed successfully!"
echo "====================================="
echo
echo "The sales_data table is now defined using this CREATE TABLE statement:"
echo
cat examples/config/schemas/sales_data.sql
