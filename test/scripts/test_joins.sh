#!/bin/bash

# Comprehensive JOIN testing script

echo "🔗 Testing JOIN Operations"
echo "=========================="

# Build binary
cd "$(dirname "$0")/../.."
go build -o ddb .

DATA_DIR="test/data"

echo ""
echo "1. Basic INNER JOIN"
echo "------------------"
./ddb query "SELECT employees.first_name, employees.last_name, departments.department_name FROM employees JOIN departments ON employees.department_id = departments.department_id" \
    --file employees:$DATA_DIR/employees.csv \
    --file departments:$DATA_DIR/departments.csv

echo ""
echo "2. JOIN with WHERE clause"
echo "------------------------"
./ddb query "SELECT first_name, last_name, department_name, salary FROM employees JOIN departments ON employees.department_id = departments.department_id WHERE salary > 90000" \
    --file employees:$DATA_DIR/employees.csv \
    --file departments:$DATA_DIR/departments.csv

echo ""
echo "3. JOIN with ORDER BY"
echo "--------------------"
./ddb query "SELECT first_name, department_name, salary FROM employees JOIN departments ON employees.department_id = departments.department_id ORDER BY salary DESC LIMIT 5" \
    --file employees:$DATA_DIR/employees.csv \
    --file departments:$DATA_DIR/departments.csv

echo ""
echo "4. Three-table JOIN simulation (employees + projects)"
echo "---------------------------------------------------"
./ddb query "SELECT first_name, last_name, project_name, status FROM employees JOIN projects ON employees.employee_id = projects.employee_id WHERE status = 'Completed'" \
    --file employees:$DATA_DIR/employees.csv \
    --file projects:$DATA_DIR/projects.csv

echo ""
echo "5. JOIN with GROUP BY"
echo "--------------------"
./ddb query "SELECT department_name FROM employees JOIN departments ON employees.department_id = departments.department_id WHERE salary > 80000 GROUP BY department_name" \
    --file employees:$DATA_DIR/employees.csv \
    --file departments:$DATA_DIR/departments.csv