#!/bin/bash

# Comprehensive SQL Function Test Suite

echo "🧮 Testing All SQL Functions"
echo "=========================="

# Build binary
cd "$(dirname "$0")/../.."
go build -o ddb .

DATA_DIR="test/data"

echo ""
echo "🔤 STRING FUNCTIONS"
echo "==================="

echo "UPPER/LOWER:"
./ddb query "SELECT UPPER(first_name) AS upper_name, LOWER(last_name) AS lower_name FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "LENGTH:"
./ddb query "SELECT first_name, LENGTH(first_name) AS name_length FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "SUBSTRING:"
./ddb query "SELECT first_name, SUBSTR(first_name, 1, 3) AS short_name FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "CONCAT (simple):"
./ddb query "SELECT CONCAT(first_name, last_name) AS full_name FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "TRIM functions:"
./ddb query "SELECT TRIM(first_name) AS trimmed, LEFT(first_name, 2) AS left_part, RIGHT(first_name, 2) AS right_part FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "🔢 MATH FUNCTIONS"
echo "================"

echo "ABS:"
./ddb query "SELECT first_name, ABS(-100) AS abs_test, ABS(salary) AS abs_salary FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "ROUND:"
./ddb query "SELECT first_name, salary, ROUND(85.7) AS rounded FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "FLOOR/CEIL:"
./ddb query "SELECT FLOOR(85.9) AS floor_test, CEIL(85.1) AS ceil_test FROM employees LIMIT 2" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "MOD:"
./ddb query "SELECT MOD(salary, 1000) AS mod_test FROM employees WHERE employee_id < 4" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "POWER/SQRT:"
./ddb query "SELECT POWER(2, 3) AS power_test, SQRT(16) AS sqrt_test FROM employees LIMIT 1" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "📅 DATE/TIME FUNCTIONS"
echo "====================="

echo "NOW/DATE:"
./ddb query "SELECT NOW() AS current_time, DATE() AS current_date FROM employees LIMIT 1" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "🔗 COMPLEX FUNCTION COMBINATIONS"
echo "==============================="

echo "Functions in WHERE:"
./ddb query "SELECT first_name FROM employees WHERE LENGTH(first_name) > 4 AND UPPER(first_name) LIKE 'j'" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "Multiple functions in SELECT:"
./ddb query "SELECT UPPER(LEFT(first_name, 1)) AS initial, LENGTH(last_name) AS last_length FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "Functions with aliases:"
./ddb query "SELECT first_name AS name, SUBSTR(first_name, 1, 1) AS initial, ABS(salary) AS abs_pay FROM employees LIMIT 3" \
    --file employees:$DATA_DIR/employees.csv