# Data Provenance and Test Dataset Documentation

## Dataset Overview

Every benchmark result references a **specific, verifiable dataset** with complete provenance tracking.

### Dataset Metadata (Captured in benchmark_results.json)

```json
{
  "dataset": {
    "rows": 2000000,
    "file": "examples/benchmark_data.csv",
    "size_mb": 123.20013618469238
  }
}
```

## Dataset Generation

### Source Script

**File**: `scripts/generate_benchmark_data.py`

**Purpose**: Generate reproducible test data

**Configuration**:
```python
NUM_ROWS = 2_000_000
OUTPUT_FILE = "examples/benchmark_data.csv"
```

**Data Generation**:
- **Products**: 10 types (Laptop, Mouse, Keyboard, Monitor, Desk, Chair, Headset, Webcam, Speaker, Tablet)
- **Regions**: 5 regions (North, South, East, West, Central)
- **Names**: 20 first names × 20 last names = 400 combinations
- **Prices**: Realistic ranges per product ($9.99 - $1,499.99)
- **Dates**: Random dates throughout 2024
- **Statuses**: 5 order statuses (shipped, pending, processing, cancelled, delivered)

### Generation Command

```bash
python3 scripts/generate_benchmark_data.py
```

**Output**:
```
Generating 2,000,000 rows of benchmark data...
Output file: examples/benchmark_data.csv
  Written 100,000 rows (5.0%)
  Written 200,000 rows (10.0%)
  ...
  Written 2,000,000 rows (100.0%)
✓ Generated 2,000,000 rows successfully!
✓ File size: 123.20 MB
```

## Dataset Structure

### Schema Definition

**File**: `.ddb/schemas/benchmark_data.sql`

```sql
CREATE TABLE benchmark_data (
    order_id INTEGER,
    customer_name STRING,
    product STRING,
    quantity INTEGER,
    price FLOAT,
    order_date DATE,
    region STRING,
    status STRING
) FILE 'examples/benchmark_data.csv' DELIMITER ',';
```

### Sample Data

**File**: `examples/benchmark_data.csv`

**Format**: CSV with header

**Example Rows**:
```csv
order_id,customer_name,product,quantity,price,order_date,region,status
1001,Richard White,Webcam,8,188.67,2024-03-25,West,delivered
1002,Richard Taylor,Mouse,6,49.58,2024-01-05,Central,pending
1003,Mary Jackson,Monitor,4,554.02,2024-05-12,South,processing
...
2001000,William Harris,Mouse,5,26.72,2024-04-15,North,delivered
```

**Statistics**:
- **Total Rows**: 2,000,001 (including header)
- **Data Rows**: 2,000,000
- **File Size**: 123.20 MB
- **Columns**: 8
- **Line Format**: Fixed CSV with comma delimiter

## Dataset Verification

### Verify Row Count

```bash
$ wc -l examples/benchmark_data.csv
2000001 examples/benchmark_data.csv  # Header + 2M data rows
```

### Verify File Size

```bash
$ du -h examples/benchmark_data.csv
124M    examples/benchmark_data.csv
```

### Verify Structure

```bash
$ head -1 examples/benchmark_data.csv
order_id,customer_name,product,quantity,price,order_date,region,status
```

### Verify Data Integrity

```bash
# Check for consistent field counts
$ awk -F',' '{print NF}' examples/benchmark_data.csv | sort -u
8  # All rows have exactly 8 fields
```

## How Benchmarks Reference the Dataset

### In benchmark_results.json

Each benchmark run captures:

```json
{
  "system_info": { ... },
  "binary": "./target/release/ddb",
  "dataset": {
    "rows": 2000000,                    ← Row count
    "file": "examples/benchmark_data.csv",  ← File path
    "size_mb": 123.20013618469238       ← Exact file size
  },
  "benchmarks": [
    {
      "name": "like_prefix",
      "query": "SELECT * FROM benchmark_data WHERE customer_name LIKE 'John%' LIMIT 1000",
      "iterations": [ ... ]
    }
  ]
}
```

### Dataset Hash (for integrity)

Generate a hash to verify dataset hasn't changed:

```bash
$ sha256sum examples/benchmark_data.csv
<hash>  examples/benchmark_data.csv
```

**Note**: The hash changes each time the dataset is regenerated (random data), but the structure and row count remain consistent.

## Benchmark Queries and Dataset

### Example 1: Filter by Region

**Query**:
```sql
SELECT * FROM benchmark_data WHERE region = 'West' LIMIT 1000
```

**What This Tests**:
- Scans 2M rows
- Filters for region = 'West' (~20% of data = 400K rows)
- Returns first 1000 matching rows

**Dataset Relevance**:
- 5 regions → ~400K rows per region
- Tests filter performance on categorical data

### Example 2: LIKE Pattern Matching

**Query**:
```sql
SELECT * FROM benchmark_data WHERE customer_name LIKE 'John%' LIMIT 1000
```

**What This Tests**:
- Scans 2M rows
- Pattern matches on customer_name field
- Tests optimized LIKE implementation

**Dataset Relevance**:
- 400 unique names (20 first × 20 last)
- "John" is 1 of 20 first names → ~5% of data = 100K rows
- Realistic string data for pattern matching

### Example 3: Sorting

**Query**:
```sql
SELECT * FROM benchmark_data ORDER BY price DESC LIMIT 100
```

**What This Tests**:
- Loads all 2M rows into memory
- Sorts by price (FLOAT column)
- Returns top 100

**Dataset Relevance**:
- Prices range from $9.99 to $1,499.99
- Realistic numeric data
- Tests memory usage (2M rows × 8 columns)

## Dataset Reproducibility

### Generate Identical Structure

To generate a new dataset with the same structure (but different random values):

```bash
python3 scripts/generate_benchmark_data.py
```

**Guarantees**:
- Same row count: 2,000,000
- Same schema: 8 columns
- Same file size: ~123 MB
- Same data distribution: Products, regions, etc.

**Differences**:
- Random values will differ
- Specific customer names will vary
- Exact prices will differ

### Generate Identical Data

For **exact reproduction** of the same dataset:

1. Save the generated CSV to a reference location
2. Copy it for benchmarks
3. Include hash verification

```bash
# After generation
sha256sum examples/benchmark_data.csv > benchmark_data.csv.sha256

# For verification
sha256sum -c benchmark_data.csv.sha256
```

## Dataset in Context

### Why This Dataset?

1. **Realistic Size**: 2M rows represents real-world medium-size data
2. **Realistic Schema**: E-commerce orders are a common use case
3. **Diverse Data Types**: STRING, INTEGER, FLOAT, DATE
4. **Pattern Variety**: Good distribution for LIKE tests
5. **Reproducible**: Anyone can generate equivalent data

### What Results Mean

When we say "LIKE 'John%' runs in 2.027 seconds":

- **Dataset**: 2,000,000 rows
- **File**: 123.20 MB CSV
- **Columns**: 8 fields per row
- **Operation**: Scans entire dataset, filters ~100K rows, returns 1000
- **Hardware**: Captured in system_info (benchmark_results.json)
- **Iterations**: 3 runs for statistical confidence

## Transparency

### What's Documented

✅ Dataset generation script (open source)
✅ Exact row count (2,000,000)
✅ File size (123.20 MB)
✅ Schema definition (8 columns with types)
✅ Sample data (visible in CSV)
✅ Data distribution (products, regions, etc.)
✅ How benchmarks query it (exact SQL in JSON)

### Independent Verification

Anyone can:

1. **Generate equivalent dataset**:
   ```bash
   python3 scripts/generate_benchmark_data.py
   ```

2. **Verify structure**:
   ```bash
   wc -l examples/benchmark_data.csv
   head examples/benchmark_data.csv
   ```

3. **Run same benchmarks**:
   ```bash
   python3 scripts/run_benchmarks.py
   ```

4. **Compare results**:
   ```bash
   cat benchmark_results.json
   ```

## Conclusion

Every benchmark claim is tied to a **specific, documented, reproducible dataset** with:

- ✅ Exact row count (2M)
- ✅ Exact file size (123.20 MB)
- ✅ Known schema (8 columns)
- ✅ Reproducible generation
- ✅ Captured in test results
- ✅ Verifiable structure
- ✅ Open source generation script

This ensures that when we claim "20x speedup", you know:
- **What data** was tested (2M row CSV)
- **How much data** (123 MB)
- **What structure** (8 columns: order_id, customer_name, etc.)
- **How to reproduce** (run generation script)
- **How to verify** (check row count, file size, run benchmarks)

---

**Generated**: 2025-10-14
**Dataset Version**: 1.0
**Last Updated**: 2025-10-14
