# Verified DDB-Rust Benchmark Results

> **Note**: These results are generated from automated, reproducible benchmarks.
> Raw data available in `benchmark_results.json` and `benchmark_results.csv`

## Test Environment

```
Hostname:       fedora
Platform:       Linux-6.16.10-200.fc42.x86_64-x86_64-with-glibc2.41
Processor:      
Test Date:      2025-10-14T19:22:36.350724
Binary:         ./target/release/ddb
```

## Dataset

- **Rows**: 2,000,000
- **File**: `examples/benchmark_data.csv`
- **Size**: 123.20 MB
- **Iterations per test**: 3

## Performance Summary

| Benchmark | Mean Time | Min Time | Max Time | Throughput |
|-----------|-----------|----------|----------|------------|
| count_all | 4.261s | 4.216s | 4.323s | - |
| filter_region | 2.672s | 2.630s | 2.698s | 748K rows/sec |
| filter_price_range | 2.145s | 2.112s | 2.164s | 932K rows/sec |
| like_prefix | 2.085s | 2.031s | 2.121s | 959K rows/sec |
| like_suffix | 3.166s | 3.101s | 3.201s | 632K rows/sec |
| like_contains | 2.353s | 2.340s | 2.365s | 850K rows/sec |
| like_complex | 44.103s | 43.190s | 45.852s | 45K rows/sec |
| sort_price | 14.205s | 14.088s | 14.407s | - |
| large_result_10k | 5.807s | 5.649s | 5.947s | 344K rows/sec |
| large_result_100k | 5.920s | 5.895s | 5.942s | 338K rows/sec |

## Aggregation Tests

### count_all

**Query**: `SELECT COUNT(*) FROM benchmark_data`

**Description**: Count all 2M rows

**Results**:

- Mean: 4.2607 seconds
- Min: 4.2162 seconds
- Max: 4.3229 seconds
- Median: 4.2430 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 4.3229s | ✅ Success |
| 2 | 4.2430s | ✅ Success |
| 3 | 4.2162s | ✅ Success |

---

## Filtering Tests

### filter_region

**Query**: `SELECT * FROM benchmark_data WHERE region = 'West' LIMIT 1000`

**Description**: Filter by region with LIMIT

**Results**:

- Mean: 2.6722 seconds
- Min: 2.6296 seconds
- Max: 2.6982 seconds
- Median: 2.6889 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 2.6982s | ✅ Success |
| 2 | 2.6296s | ✅ Success |
| 3 | 2.6889s | ✅ Success |

### filter_price_range

**Query**: `SELECT * FROM benchmark_data WHERE price > 500 AND price < 600 LIMIT 1000`

**Description**: Filter by price range

**Results**:

- Mean: 2.1455 seconds
- Min: 2.1119 seconds
- Max: 2.1638 seconds
- Median: 2.1607 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 2.1119s | ✅ Success |
| 2 | 2.1607s | ✅ Success |
| 3 | 2.1638s | ✅ Success |

---

## Pattern Matching Tests

### like_prefix

**Query**: `SELECT * FROM benchmark_data WHERE customer_name LIKE 'John%' LIMIT 1000`

**Description**: LIKE prefix match (optimized)

**Results**:

- Mean: 2.0847 seconds
- Min: 2.0314 seconds
- Max: 2.1212 seconds
- Median: 2.1016 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 2.0314s | ✅ Success |
| 2 | 2.1212s | ✅ Success |
| 3 | 2.1016s | ✅ Success |

### like_suffix

**Query**: `SELECT * FROM benchmark_data WHERE customer_name LIKE '%son' LIMIT 1000`

**Description**: LIKE suffix match (optimized)

**Results**:

- Mean: 3.1657 seconds
- Min: 3.1007 seconds
- Max: 3.2011 seconds
- Median: 3.1952 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 3.1952s | ✅ Success |
| 2 | 3.2011s | ✅ Success |
| 3 | 3.1007s | ✅ Success |

### like_contains

**Query**: `SELECT * FROM benchmark_data WHERE customer_name LIKE '%John%' LIMIT 1000`

**Description**: LIKE contains match (optimized)

**Results**:

- Mean: 2.3530 seconds
- Min: 2.3401 seconds
- Max: 2.3651 seconds
- Median: 2.3538 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 2.3538s | ✅ Success |
| 2 | 2.3401s | ✅ Success |
| 3 | 2.3651s | ✅ Success |

### like_complex

**Query**: `SELECT * FROM benchmark_data WHERE customer_name LIKE 'J_hn%' LIMIT 1000`

**Description**: LIKE with underscore wildcard (regex)

**Results**:

- Mean: 44.1026 seconds
- Min: 43.1898 seconds
- Max: 45.8523 seconds
- Median: 43.2658 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 45.8523s | ✅ Success |
| 2 | 43.2658s | ✅ Success |
| 3 | 43.1898s | ✅ Success |

---

## Scanning Tests

### large_result_10k

**Query**: `SELECT * FROM benchmark_data LIMIT 10000`

**Description**: Read 10K rows

**Results**:

- Mean: 5.8070 seconds
- Min: 5.6487 seconds
- Max: 5.9472 seconds
- Median: 5.8250 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 5.9472s | ✅ Success |
| 2 | 5.8250s | ✅ Success |
| 3 | 5.6487s | ✅ Success |

### large_result_100k

**Query**: `SELECT * FROM benchmark_data LIMIT 100000`

**Description**: Read 100K rows

**Results**:

- Mean: 5.9197 seconds
- Min: 5.8954 seconds
- Max: 5.9423 seconds
- Median: 5.9215 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 5.9423s | ✅ Success |
| 2 | 5.8954s | ✅ Success |
| 3 | 5.9215s | ✅ Success |

---

## Sorting Tests

### sort_price

**Query**: `SELECT * FROM benchmark_data ORDER BY price DESC LIMIT 100`

**Description**: Sort by price descending

**Results**:

- Mean: 14.2049 seconds
- Min: 14.0878 seconds
- Max: 14.4073 seconds
- Median: 14.1196 seconds

**Individual Runs**:

| Run | Duration | Status |
|-----|----------|--------|
| 1 | 14.1196s | ✅ Success |
| 2 | 14.0878s | ✅ Success |
| 3 | 14.4073s | ✅ Success |

---

## LIKE Pattern Matching Performance

Comparison of optimized LIKE patterns:

| Pattern Type | Query | Mean Time | Throughput |
|--------------|-------|-----------|------------|
| Prefix | `LIKE 'John%'` | 2.085s | 959K rows/sec |
| Suffix | `LIKE '%son'` | 3.166s | 632K rows/sec |
| Contains | `LIKE '%John%'` | 2.353s | 850K rows/sec |
| Complex | `LIKE 'J_hn%'` | 44.103s | 45K rows/sec |

**Note**: These results demonstrate the 20x speedup from LIKE optimization.
The historical pre-optimization time for `LIKE 'John%'` was **40.910 seconds**.

## Reproducibility

To reproduce these results:

```bash
# 1. Build the release binary
cargo build --release

# 2. Generate benchmark data
python3 scripts/generate_benchmark_data.py

# 3. Run benchmarks
python3 scripts/run_benchmarks.py

# 4. Generate this report
python3 scripts/analyze_benchmarks.py
```

**Generated**: 2025-10-14T19:27:30.641381
