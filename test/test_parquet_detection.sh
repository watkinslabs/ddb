#!/bin/bash

# Test script for Parquet format detection
# This test verifies that the CLI can detect .parquet files correctly

set -e

echo "🔍 Testing Parquet Format Detection"
echo "==================================="

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
run_format_test() {
    local test_name="$1"
    local file_extension="$2"
    local expected_format="$3"
    
    echo -e "${BLUE}Testing: $test_name${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # Create a temporary file with the specified extension
    local temp_file="/tmp/test_file$file_extension"
    echo "col1,col2,col3" > "$temp_file"
    echo "1,2,3" >> "$temp_file"
    
    # Try to create a config for this file and check the detected format
    local config_output=$(./ddb config create test_table "$temp_file" --auto-detect 2>&1 || true)
    
    # Clean up temp file
    rm -f "$temp_file"
    rm -f "test_table.yaml"
    
    # Check if the expected format was detected
    if echo "$config_output" | grep -q "Format: $expected_format"; then
        echo -e "${GREEN}✅ PASSED${NC} (Format: $expected_format)"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}❌ FAILED${NC} Expected format: $expected_format"
        echo "Output: $config_output"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo ""
}

# Test format detection for various file types
run_format_test "CSV format detection" ".csv" "csv"
run_format_test "JSON format detection" ".json" "json"
run_format_test "JSONL format detection" ".jsonl" "jsonl"
run_format_test "YAML format detection" ".yaml" "yaml"
run_format_test "Parquet format detection" ".parquet" "parquet"
run_format_test "TSV format detection" ".tsv" "csv"

echo ""
echo "📊 Format Detection Test Results"
echo "==============================="
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All format detection tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some format detection tests failed!${NC}"
    exit 1
fi