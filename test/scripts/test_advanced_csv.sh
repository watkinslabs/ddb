#!/bin/bash

# Test advanced CSV parsing features

echo "📋 Testing Advanced CSV Features"
echo "================================"

# Build binary
cd "$(dirname "$0")/../.."
go build -o ddb .

DATA_DIR="test/data"

echo ""
echo "1. Colon-Delimited CSV with Max Columns"
echo "---------------------------------------"
echo "Problem: URLs contain colons that would normally split into extra columns"
echo "Solution: Use --max-columns to limit parsing"
echo ""

echo "WITHOUT max-columns (splits URLs incorrectly):"
./ddb query "SELECT * FROM complex" \
    --file complex:$DATA_DIR/complex_csv.csv \
    --delimiter ":"

echo ""
echo "WITH max-columns=4 (keeps URLs intact):"
./ddb query "SELECT name, description, website FROM complex" \
    --file complex:$DATA_DIR/complex_csv.csv \
    --delimiter ":" \
    --max-columns 4

echo ""
echo "2. Quote Handling Test"
echo "---------------------"
echo "Standard CSV with quotes and commas:"
./ddb query "SELECT * FROM departments" \
    --file departments:$DATA_DIR/departments.csv \
    --allow-quoted

echo ""
echo "3. Custom Delimiter Examples"
echo "----------------------------"

# Create pipe-delimited test data
cat > test/data/pipe_test.csv << 'EOF'
id|name|role|salary
1|John Doe|Software Engineer|75000
2|Jane Smith|Product Manager|85000
3|Bob Johnson|Designer|65000
EOF

echo "Pipe-delimited data:"
./ddb query "SELECT name, role, salary FROM pipe_data WHERE salary > 70000" \
    --file pipe_data:test/data/pipe_test.csv \
    --delimiter "|"

echo ""
echo "4. Tab-Delimited Data"
echo "--------------------"

# Create tab-delimited test data  
printf "id\tproduct\tdescription\tprice\n1\tLaptop\tHigh-performance laptop\t1299.99\n2\tMouse\tWireless mouse\t29.99\n" > test/data/tab_test.csv

echo "Tab-delimited data:"
./ddb query "SELECT product, price FROM tab_data WHERE price > 100" \
    --file tab_data:test/data/tab_test.csv \
    --delimiter $'\t'

echo ""
echo "5. Mixed Format Handling"
echo "-----------------------"
echo "Spaces around values (with trim):"
./ddb query "SELECT * FROM departments" \
    --file departments:$DATA_DIR/departments.csv \
    --trim-spaces

echo ""
echo "6. Real-World Scenario: Log File with Colons"
echo "--------------------------------------------"

# Create a log-like file with multiple colons
cat > test/data/log_data.csv << 'EOF'
timestamp:level:service:message
2023-01-01 10:00:00:INFO:web-server:Server started on http://localhost:8080
2023-01-01 10:01:00:ERROR:database:Connection failed to postgres://db:5432/app
2023-01-01 10:02:00:WARN:api:Rate limit exceeded for user:12345
EOF

echo "Log data with multiple colons (max-columns=4):"
./ddb query "SELECT level, service, message FROM logs WHERE level = 'ERROR'" \
    --file logs:test/data/log_data.csv \
    --delimiter ":" \
    --max-columns 4

# Clean up temp files
rm -f test/data/pipe_test.csv test/data/tab_test.csv test/data/log_data.csv