#!/bin/bash

# DDB v2 Comprehensive Benchmark Runner
# Runs all benchmarks and generates HTML reports with graphs

set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          DDB v2 Comprehensive Benchmark Suite                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print section headers
print_section() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE} $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Check if cargo is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed"
    exit 1
fi

# Build in release mode first
print_section "Building DDB in release mode..."
cargo build --release
echo -e "${GREEN}✓ Build complete${NC}"

# Run main benchmarks
print_section "Running CRUD Operation Benchmarks..."
echo "This suite tests:"
echo "  • Tokenization (parsing SQL queries)"
echo "  • SELECT operations (full scan, WHERE, ORDER BY, LIMIT)"
echo "  • Aggregations (COUNT, SUM, AVG, GROUP BY, HAVING)"
echo "  • JOIN operations (INNER, LEFT)"
echo "  • INSERT operations (single and batch)"
echo "  • UPDATE operations (single and multiple rows)"
echo "  • DELETE operations (single and multiple rows)"
echo "  • UPSERT operations (insert new and update existing)"
echo ""
cargo bench --bench benchmarks
echo -e "${GREEN}✓ CRUD benchmarks complete${NC}"

# Run concurrency benchmarks
print_section "Running Concurrency & File Locking Benchmarks..."
echo "This suite tests:"
echo "  • Concurrent reads (multiple threads with shared locks)"
echo "  • Sequential writes (exclusive lock acquisition)"
echo "  • Concurrent inserts (file locking overhead)"
echo "  • UPSERT locking (read + write operations)"
echo "  • DELETE locking (full file rewrite)"
echo "  • Mixed read/write workloads"
echo ""
cargo bench --bench concurrency_benchmark
echo -e "${GREEN}✓ Concurrency benchmarks complete${NC}"

# Print results summary
print_section "Benchmark Results"
echo "Results have been generated in: target/criterion/"
echo ""
echo "To view the visual reports:"
echo "  1. Main benchmarks:"
echo "     ${YELLOW}open target/criterion/report/index.html${NC}"
echo ""
echo "  2. Individual benchmark groups:"
echo "     • Tokenization:    target/criterion/tokenization/report/index.html"
echo "     • SELECT:          target/criterion/select/report/index.html"
echo "     • Aggregation:     target/criterion/aggregation/report/index.html"
echo "     • JOIN:            target/criterion/join/report/index.html"
echo "     • INSERT:          target/criterion/insert/report/index.html"
echo "     • UPDATE:          target/criterion/update/report/index.html"
echo "     • DELETE:          target/criterion/delete/report/index.html"
echo "     • UPSERT:          target/criterion/upsert/report/index.html"
echo "     • Concurrent reads:    target/criterion/concurrent_reads/report/index.html"
echo "     • Sequential writes:   target/criterion/sequential_writes/report/index.html"
echo "     • Concurrent inserts:  target/criterion/concurrent_inserts/report/index.html"
echo "     • Mixed workload:      target/criterion/mixed_workload/report/index.html"
echo ""

# Check if running on a system with a browser
if command -v xdg-open &> /dev/null; then
    echo "Opening main report in browser..."
    xdg-open target/criterion/report/index.html 2>/dev/null || true
elif command -v open &> /dev/null; then
    echo "Opening main report in browser..."
    open target/criterion/report/index.html 2>/dev/null || true
fi

print_section "Benchmark Summary"
echo "Benchmark suite completed successfully!"
echo ""
echo "Key Metrics Tested:"
echo "  ✓ Tokenization speed (simple and complex SQL)"
echo "  ✓ SELECT performance across 100, 1K, 10K rows"
echo "  ✓ Aggregation operations (COUNT, SUM, AVG, GROUP BY, HAVING)"
echo "  ✓ JOIN performance (INNER, LEFT) with varying data sizes"
echo "  ✓ INSERT batch operations (1, 10, 100 rows)"
echo "  ✓ UPDATE single and multiple row operations"
echo "  ✓ DELETE single and multiple row operations"
echo "  ✓ UPSERT insert-new and update-existing scenarios"
echo "  ✓ Concurrent read operations (2, 4, 8 threads)"
echo "  ✓ Sequential write operations with file locking"
echo "  ✓ Mixed read/write workloads"
echo ""
echo -e "${GREEN}All benchmarks completed! 🚀${NC}"
echo ""
