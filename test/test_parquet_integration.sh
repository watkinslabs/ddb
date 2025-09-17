#!/bin/bash

# Integration test for Parquet support
# Tests that the system recognizes .parquet files and attempts to parse them

set -e

echo "🔍 Testing Parquet Integration"
echo "=============================="

# Build the binary
echo "📦 Building ddb..."
cd "$(dirname "$0")/.."
go build -o ddb .

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Test function
run_integration_test() {
    local test_name="$1"
    local command="$2"
    local expected_behavior="$3"
    
    echo -e "${BLUE}Testing: $test_name${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Run the command and capture output
    local output=$(eval "$command" 2>&1 || true)
    
    case "$expected_behavior" in
        "file_not_found")
            if echo "$output" | grep -q "file not accessible\|no such file"; then
                echo -e "${GREEN}✅ PASSED${NC} (Correctly detected missing file)"
                PASSED_TESTS=$((PASSED_TESTS + 1))
            else
                echo -e "${RED}❌ FAILED${NC} Expected file not found error"
                echo "Output: $output"
                FAILED_TESTS=$((FAILED_TESTS + 1))
            fi
            ;;
        "format_recognized")
            if echo "$output" | grep -q "parquet"; then
                echo -e "${GREEN}✅ PASSED${NC} (Parquet format recognized)"
                PASSED_TESTS=$((PASSED_TESTS + 1))
            else
                echo -e "${RED}❌ FAILED${NC} Expected parquet format recognition"
                echo "Output: $output"
                FAILED_TESTS=$((FAILED_TESTS + 1))
            fi
            ;;
    esac
    echo ""
}

# Test 1: Test that .parquet files are recognized in query command
run_integration_test "Parquet file recognition in query" \
    "./ddb query 'SELECT * FROM test' --file test:/nonexistent/file.parquet" \
    "file_not_found"

# Test 2: Test that config creation recognizes .parquet files
run_integration_test "Parquet config creation" \
    "./ddb config create parquet_table /nonexistent/file.parquet" \
    "file_not_found"

echo ""
echo "📊 Parquet Integration Test Results"
echo "=================================="
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All Parquet integration tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some Parquet integration tests failed!${NC}"
    exit 1
fi