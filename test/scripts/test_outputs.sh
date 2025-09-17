#!/bin/bash

# Test all output formats

echo "📄 Testing Output Formats"
echo "========================="

# Build binary
cd "$(dirname "$0")/../.."
go build -o ddb .

DATA_DIR="test/data"
OUTPUT_DIR="test/output"

mkdir -p "$OUTPUT_DIR"

echo ""
echo "1. CSV Output"
echo "------------"
./ddb query "SELECT first_name, last_name, salary FROM employees WHERE salary > 90000 ORDER BY salary DESC" \
    --file employees:$DATA_DIR/employees.csv \
    --output csv \
    --export $OUTPUT_DIR/high_earners.csv

echo "Generated: $OUTPUT_DIR/high_earners.csv"
echo "Content preview:"
head -5 "$OUTPUT_DIR/high_earners.csv"

echo ""
echo "2. JSON Output"
echo "-------------"
./ddb query "SELECT department_name, location, budget FROM departments ORDER BY budget DESC" \
    --file departments:$DATA_DIR/departments.csv \
    --output json \
    --export $OUTPUT_DIR/departments.json

echo "Generated: $OUTPUT_DIR/departments.json"
echo "Content preview:"
head -10 "$OUTPUT_DIR/departments.json"

echo ""
echo "3. YAML Output"
echo "-------------"
./ddb query "SELECT name, price, category FROM products WHERE category = 'Electronics' ORDER BY price DESC" \
    --file products:$DATA_DIR/products.json \
    --output yaml \
    --export $OUTPUT_DIR/electronics.yaml

echo "Generated: $OUTPUT_DIR/electronics.yaml"
echo "Content preview:"
head -15 "$OUTPUT_DIR/electronics.yaml"

echo ""
echo "4. JSONL Output"
echo "--------------"
./ddb query "SELECT sale_id, product_id, amount FROM sales WHERE amount > 500" \
    --file sales:$DATA_DIR/sales.jsonl \
    --output jsonl \
    --export $OUTPUT_DIR/large_sales.jsonl

echo "Generated: $OUTPUT_DIR/large_sales.jsonl"
echo "Content preview:"
head -5 "$OUTPUT_DIR/large_sales.jsonl"

echo ""
echo "5. Table Output (Console)"
echo "------------------------"
echo "Regular table format:"
./ddb query "SELECT first_name, department_id, salary FROM employees WHERE department_id = 1 ORDER BY salary DESC LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "📊 File Sizes Generated:"
ls -lh $OUTPUT_DIR/