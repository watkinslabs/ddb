#!/bin/bash
# Benchmark script for DDB with 2 million row dataset

DDB="./target/release/ddb"
ROWS=2000000

echo "========================================="
echo "DDB Performance Benchmark"
echo "========================================="
echo "Dataset: 2,000,000 rows (~123 MB)"
echo ""

# Check if benchmark data exists
if [ ! -f "benchmarks/data/benchmark_data.csv" ]; then
    echo "Error: Benchmark data not found. Run: python3 benchmarks/scripts/generate_benchmark_data.py"
    exit 1
fi

# Function to run a benchmark query
benchmark_query() {
    local name="$1"
    local query="$2"

    echo -n "  $name... "

    # Run with time and capture output
    start=$(date +%s.%N)
    result=$($DDB --query "$query" 2>&1)
    end=$(date +%s.%N)

    # Calculate duration
    duration=$(echo "$end - $start" | bc)

    # Count rows returned
    rows=$(echo "$result" | grep -c "^|" || echo "0")
    if [ "$rows" -gt 2 ]; then
        rows=$((rows - 3))  # Subtract header and separator lines
    fi

    printf "%.3fs" "$duration"
    if [ "$rows" -gt 0 ]; then
        printf " (%d rows)" "$rows"
    fi
    echo ""
}

echo "Test 1: Full Table Scan"
echo "------------------------"
benchmark_query "Count all rows" "SELECT COUNT(*) FROM benchmark_data"
echo ""

echo "Test 2: Filtering (WHERE clause)"
echo "--------------------------------"
benchmark_query "Filter by region" "SELECT * FROM benchmark_data WHERE region = 'West' LIMIT 1000"
benchmark_query "Filter by price range" "SELECT * FROM benchmark_data WHERE price > 500 AND price < 600 LIMIT 1000"
benchmark_query "Filter by status" "SELECT * FROM benchmark_data WHERE status = 'shipped' LIMIT 1000"
echo ""

echo "Test 3: Sorting (ORDER BY)"
echo "--------------------------"
benchmark_query "Sort by price" "SELECT * FROM benchmark_data ORDER BY price DESC LIMIT 100"
benchmark_query "Sort by date" "SELECT * FROM benchmark_data ORDER BY order_date DESC LIMIT 100"
benchmark_query "Sort by multiple columns" "SELECT * FROM benchmark_data ORDER BY region, price DESC LIMIT 100"
echo ""

echo "Test 4: String Operations"
echo "-------------------------"
benchmark_query "LIKE pattern match" "SELECT * FROM benchmark_data WHERE customer_name LIKE 'John%' LIMIT 1000"
benchmark_query "UPPER function" "SELECT UPPER(product), price FROM benchmark_data LIMIT 1000"
echo ""

echo "Test 5: Aggregations"
echo "--------------------"
# Note: Aggregations might fail without GROUP BY support, but let's try
benchmark_query "MIN price" "SELECT MIN(price) FROM benchmark_data LIMIT 1"
benchmark_query "MAX price" "SELECT MAX(price) FROM benchmark_data LIMIT 1"
benchmark_query "AVG price" "SELECT AVG(price) FROM benchmark_data LIMIT 1"
echo ""

echo "Test 6: Complex Queries"
echo "-----------------------"
benchmark_query "Multi-filter with sort" "SELECT product, price, region FROM benchmark_data WHERE price > 100 AND region = 'North' ORDER BY price DESC LIMIT 500"
benchmark_query "Date filter with sort" "SELECT customer_name, order_date, price FROM benchmark_data WHERE order_date > '2024-06-01' ORDER BY order_date LIMIT 500"
echo ""

echo "Test 7: Large Result Sets"
echo "-------------------------"
benchmark_query "10K rows" "SELECT * FROM benchmark_data LIMIT 10000"
benchmark_query "50K rows" "SELECT * FROM benchmark_data LIMIT 50000"
benchmark_query "100K rows" "SELECT * FROM benchmark_data LIMIT 100000"
echo ""

echo "========================================="
echo "Benchmark Complete"
echo "========================================="
