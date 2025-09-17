#!/bin/bash

# Agent-DDB Test Suite
# Tests all major functionality of the SQL engine

set -e  # Exit on any error

echo "🚀 Starting Agent-DDB Test Suite"
echo "================================="

# Build the binary
echo "📦 Building ddb..."
cd "$(dirname "$0")/.."
go build -o ddb .

# Test directory
TEST_DIR="$(dirname "$0")"
DATA_DIR="$TEST_DIR/data"
EXPECTED_DIR="$TEST_DIR/expected"
OUTPUT_DIR="$TEST_DIR/output"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Test function
run_test() {
    local test_name="$1"
    local command="$2"
    local expected_rows="$3"
    
    echo -e "${BLUE}Testing: $test_name${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Run the command and capture output
    if eval "$command" > "$OUTPUT_DIR/test_output.tmp" 2>&1; then
        # Extract row count from output like "(10 rows)"
        local actual_rows=$(grep -o '([0-9]\+ rows)' "$OUTPUT_DIR/test_output.tmp" | grep -o '[0-9]\+' || echo "0")
        
        if [ "$actual_rows" = "$expected_rows" ]; then
            echo -e "${GREEN}✅ PASSED${NC} ($actual_rows rows)"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}❌ FAILED${NC} Expected $expected_rows rows, got $actual_rows"
            echo "Command: $command"
            cat "$OUTPUT_DIR/test_output.tmp"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        echo -e "${RED}❌ FAILED${NC} Command execution failed"
        echo "Command: $command"
        cat "$OUTPUT_DIR/test_output.tmp"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
}

# Test function for output validation
run_output_test() {
    local test_name="$1"
    local command="$2"
    local output_file="$3"
    
    echo -e "${BLUE}Testing: $test_name${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Run the command
    if eval "$command" > /dev/null 2>&1; then
        if [ -f "$output_file" ]; then
            echo -e "${GREEN}✅ PASSED${NC} Output file created: $output_file"
            echo "File size: $(wc -c < "$output_file") bytes"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}❌ FAILED${NC} Output file not created: $output_file"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        echo -e "${RED}❌ FAILED${NC} Command execution failed"
        echo "Command: $command"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
}

echo "🔍 Running Basic Query Tests"
echo "----------------------------"

# Basic SELECT tests
run_test "Basic SELECT *" \
    "./ddb query 'SELECT * FROM employees' --file employees:$DATA_DIR/employees.csv" \
    "10"

run_test "SELECT specific columns" \
    "./ddb query 'SELECT first_name, last_name, salary FROM employees' --file employees:$DATA_DIR/employees.csv" \
    "10"

run_test "SELECT with column ordering" \
    "./ddb query 'SELECT salary, first_name, department_id FROM employees' --file employees:$DATA_DIR/employees.csv" \
    "10"

echo "🔍 Running WHERE Clause Tests" 
echo "-----------------------------"

# WHERE clause tests
run_test "WHERE with number comparison" \
    "./ddb query 'SELECT * FROM employees WHERE salary > 80000' --file employees:$DATA_DIR/employees.csv" \
    "6"

run_test "WHERE with string comparison" \
    "./ddb query 'SELECT * FROM employees WHERE department_id = 1' --file employees:$DATA_DIR/employees.csv" \
    "4"

run_test "WHERE with AND condition" \
    "./ddb query 'SELECT * FROM employees WHERE department_id = 1 AND salary > 70000' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "WHERE with OR condition" \
    "./ddb query 'SELECT * FROM employees WHERE salary > 100000 OR department_id = 3' --file employees:$DATA_DIR/employees.csv" \
    "5"

echo "🔍 Running JOIN Tests"
echo "--------------------"

# JOIN tests
run_test "INNER JOIN" \
    "./ddb query 'SELECT * FROM employees JOIN departments ON employees.department_id = departments.department_id' --file employees:$DATA_DIR/employees.csv --file departments:$DATA_DIR/departments.csv" \
    "10"

run_test "JOIN with specific columns" \
    "./ddb query 'SELECT first_name, last_name, department_name FROM employees JOIN departments ON employees.department_id = departments.department_id' --file employees:$DATA_DIR/employees.csv --file departments:$DATA_DIR/departments.csv" \
    "10"

run_test "JOIN with WHERE" \
    "./ddb query 'SELECT first_name, department_name FROM employees JOIN departments ON employees.department_id = departments.department_id WHERE salary > 90000' --file employees:$DATA_DIR/employees.csv --file departments:$DATA_DIR/departments.csv" \
    "4"

echo "🔍 Running ORDER BY and LIMIT Tests"
echo "-----------------------------------"

# ORDER BY tests
run_test "ORDER BY salary ASC" \
    "./ddb query 'SELECT first_name, salary FROM employees ORDER BY salary' --file employees:$DATA_DIR/employees.csv" \
    "10"

run_test "ORDER BY salary DESC" \
    "./ddb query 'SELECT first_name, salary FROM employees ORDER BY salary DESC' --file employees:$DATA_DIR/employees.csv" \
    "10"

run_test "LIMIT without ORDER BY" \
    "./ddb query 'SELECT * FROM employees LIMIT 5' --file employees:$DATA_DIR/employees.csv" \
    "5"

run_test "ORDER BY with LIMIT" \
    "./ddb query 'SELECT first_name, salary FROM employees ORDER BY salary DESC LIMIT 3' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "LIMIT with OFFSET" \
    "./ddb query 'SELECT first_name FROM employees ORDER BY employee_id LIMIT 2,3' --file employees:$DATA_DIR/employees.csv" \
    "3"

echo "🔍 Running GROUP BY Tests"
echo "-------------------------"

# GROUP BY tests 
run_test "GROUP BY department" \
    "./ddb query 'SELECT department_id FROM employees GROUP BY department_id' --file employees:$DATA_DIR/employees.csv" \
    "3"

echo "🔍 Running Multi-Format Tests"
echo "-----------------------------"

# Different file format tests
run_test "JSON file query" \
    "./ddb query 'SELECT name, price FROM products WHERE price > 300' --file products:$DATA_DIR/products.json" \
    "3"

run_test "JSONL file query" \
    "./ddb query 'SELECT * FROM sales WHERE amount > 1000' --file sales:$DATA_DIR/sales.jsonl" \
    "3"

echo "🔍 Running Advanced CSV Tests"  
echo "-----------------------------"

# Advanced CSV parsing tests
run_test "Colon-delimited CSV with max columns" \
    "./ddb query 'SELECT name, website FROM complex' --file complex:$DATA_DIR/complex_csv.csv --delimiter ':' --max-columns 4" \
    "4"

echo "🔍 Running Output Format Tests"
echo "------------------------------"

# Output format tests
run_output_test "CSV output" \
    "./ddb query 'SELECT first_name, last_name, salary FROM employees WHERE salary > 100000' --file employees:$DATA_DIR/employees.csv --output csv --export $OUTPUT_DIR/high_earners.csv" \
    "$OUTPUT_DIR/high_earners.csv"

run_output_test "JSON output" \
    "./ddb query 'SELECT * FROM departments' --file departments:$DATA_DIR/departments.csv --output json --export $OUTPUT_DIR/departments.json" \
    "$OUTPUT_DIR/departments.json"

run_output_test "YAML output" \
    "./ddb query 'SELECT name, price FROM products ORDER BY price DESC LIMIT 3' --file products:$DATA_DIR/products.json --output yaml --export $OUTPUT_DIR/top_products.yaml" \
    "$OUTPUT_DIR/top_products.yaml"

echo "🔍 Running Expression and Function Tests"
echo "---------------------------------------"

# String function tests
run_test "UPPER/LOWER functions" \
    "./ddb query 'SELECT UPPER(first_name), LOWER(last_name) FROM employees LIMIT 3' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "LENGTH function" \
    "./ddb query 'SELECT LENGTH(first_name) AS name_length FROM employees WHERE LENGTH(first_name) > 4 LIMIT 3' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "SUBSTRING function" \
    "./ddb query 'SELECT SUBSTR(first_name, 1, 3) FROM employees LIMIT 3' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "CONCAT function" \
    "./ddb query 'SELECT CONCAT(first_name, last_name) FROM employees LIMIT 3' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "TRIM/LEFT/RIGHT functions" \
    "./ddb query 'SELECT LEFT(first_name, 2), RIGHT(first_name, 2) FROM employees LIMIT 3' --file employees:$DATA_DIR/employees.csv" \
    "3"

# Math function tests
run_test "ABS function" \
    "./ddb query 'SELECT ABS(-100), ABS(salary) FROM employees LIMIT 2' --file employees:$DATA_DIR/employees.csv" \
    "2"

run_test "ROUND function" \
    "./ddb query 'SELECT ROUND(85.7) FROM employees LIMIT 1' --file employees:$DATA_DIR/employees.csv" \
    "1"

run_test "FLOOR/CEIL functions" \
    "./ddb query 'SELECT FLOOR(85.9), CEIL(85.1) FROM employees LIMIT 1' --file employees:$DATA_DIR/employees.csv" \
    "1"

run_test "POWER/SQRT functions" \
    "./ddb query 'SELECT POWER(2, 3), SQRT(16) FROM employees LIMIT 1' --file employees:$DATA_DIR/employees.csv" \
    "1"

# Date function tests
run_test "DATE functions" \
    "./ddb query 'SELECT NOW(), DATE() FROM employees LIMIT 1' --file employees:$DATA_DIR/employees.csv" \
    "1"

# Function tests in WHERE clause
run_test "Function in WHERE clause" \
    "./ddb query 'SELECT first_name FROM employees WHERE UPPER(first_name) = \"JOHN\"' --file employees:$DATA_DIR/employees.csv" \
    "1"

# Alias tests
run_test "Column aliases in SELECT" \
    "./ddb query 'SELECT first_name AS name, salary AS pay FROM employees WHERE salary > 100000' --file employees:$DATA_DIR/employees.csv" \
    "3"

# Pattern matching tests
run_test "LIKE starts with pattern" \
    "./ddb query 'SELECT first_name FROM employees WHERE first_name LIKE \"J%\"' --file employees:$DATA_DIR/employees.csv" \
    "2"

run_test "LIKE ends with pattern" \
    "./ddb query 'SELECT first_name FROM employees WHERE first_name LIKE \"%e\"' --file employees:$DATA_DIR/employees.csv" \
    "4"

run_test "LIKE contains pattern" \
    "./ddb query 'SELECT first_name FROM employees WHERE first_name LIKE \"%an%\"' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "LIKE single character wildcard" \
    "./ddb query 'SELECT first_name FROM employees WHERE first_name LIKE \"J_n%\"' --file employees:$DATA_DIR/employees.csv" \
    "1"

# NULL tests
run_test "IS NULL test" \
    "./ddb query 'SELECT first_name FROM employees WHERE manager_id IS NULL' --file employees:$DATA_DIR/employees.csv" \
    "3"

run_test "IS NOT NULL test" \
    "./ddb query 'SELECT first_name FROM employees WHERE manager_id IS NOT NULL' --file employees:$DATA_DIR/employees.csv" \
    "7"

# Complex WHERE expressions
run_test "IN expression" \
    "./ddb query 'SELECT first_name FROM employees WHERE department_id IN (1, 3)' --file employees:$DATA_DIR/employees.csv" \
    "7"

run_test "Complex JOIN with WHERE expressions" \
    "./ddb query 'SELECT e.first_name, d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE e.salary > 90000 AND d.department_name = \"Engineering\"' --file employees:$DATA_DIR/employees.csv --file departments:$DATA_DIR/departments.csv" \
    "1"

echo "🔍 Running Complex Query Tests"
echo "------------------------------"

# Complex queries
run_test "Multi-table JOIN with aggregation" \
    "./ddb query 'SELECT department_name FROM employees JOIN departments ON employees.department_id = departments.department_id WHERE salary > 80000 GROUP BY department_name' --file employees:$DATA_DIR/employees.csv --file departments:$DATA_DIR/departments.csv" \
    "3"

run_test "Three-way data correlation" \
    "./ddb query 'SELECT first_name, project_name FROM employees JOIN projects ON employees.employee_id = projects.employee_id WHERE status = \"Completed\"' --file employees:$DATA_DIR/employees.csv --file projects:$DATA_DIR/projects.csv" \
    "3"

echo "🔍 Running Parquet Support Tests"
echo "--------------------------------"

# Run Parquet-specific tests
echo -e "${BLUE}Running Parquet format detection tests...${NC}"
if ./test/test_parquet_detection.sh > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Parquet format detection tests passed${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}❌ Parquet format detection tests failed${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))
echo ""

echo -e "${BLUE}Running Parquet integration tests...${NC}"
if ./test/test_parquet_integration.sh > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Parquet integration tests passed${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}❌ Parquet integration tests failed${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))
echo ""

echo ""
echo "📊 Test Results Summary"
echo "======================"
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some tests failed!${NC}"
    exit 1
fi