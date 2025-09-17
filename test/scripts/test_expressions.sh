#!/bin/bash

# Test WHERE clause expressions and functions

echo "🧮 Testing WHERE Expressions and Functions"
echo "==========================================="

# Build binary
cd "$(dirname "$0")/../.."
go build -o ddb .

DATA_DIR="test/data"

echo ""
echo "1. Numeric Comparisons"
echo "---------------------"
echo "Salary > 80000:"
./ddb query "SELECT first_name, last_name, salary FROM employees WHERE salary > 80000" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "Salary BETWEEN (using AND):"
./ddb query "SELECT first_name, salary FROM employees WHERE salary > 70000 AND salary < 100000" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "2. String Comparisons"
echo "--------------------"
echo "Department ID equals:"
./ddb query "SELECT first_name, department_id FROM employees WHERE department_id = 1" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "3. Complex Boolean Logic"
echo "-----------------------"
echo "OR condition:"
./ddb query "SELECT first_name, salary, department_id FROM employees WHERE salary > 100000 OR department_id = 3" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "Multiple AND conditions:"
./ddb query "SELECT first_name, last_name FROM employees WHERE department_id = 1 AND salary > 70000 AND employee_id < 5" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "4. Column Ordering Tests"
echo "-----------------------"
echo "Different column order:"
./ddb query "SELECT salary, department_id, first_name, employee_id FROM employees WHERE salary > 90000" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "5. NULL/Empty Handling"
echo "---------------------"
echo "Employees with no manager (empty manager_id):"
./ddb query "SELECT first_name, last_name, manager_id FROM employees WHERE manager_id = ''" \
    --file employees:$DATA_DIR/employees.csv

echo ""
echo "6. Product Price Tests (from JSON)"
echo "---------------------------------"
echo "Products over $400:"
./ddb query "SELECT name, price, category FROM products WHERE price > 400" \
    --file products:$DATA_DIR/products.json

echo ""
echo "Electronics category:"
./ddb query "SELECT name, price FROM products WHERE category = 'Electronics'" \
    --file products:$DATA_DIR/products.json