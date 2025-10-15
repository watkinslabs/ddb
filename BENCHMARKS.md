# DDB v2 Benchmark Suite

Comprehensive performance benchmarks for all SQL operations and concurrency scenarios.

## Quick Start

```bash
# Run all benchmarks and generate HTML reports
./run_benchmarks.sh

# Run specific benchmark suite
cargo bench --bench benchmarks           # CRUD operations
cargo bench --bench concurrency_benchmark # File locking tests

# Run specific benchmark group
cargo bench --bench benchmarks select
cargo bench --bench benchmarks aggregation
cargo bench --bench benchmarks join
```

## Benchmark Suites

### 1. CRUD Operations (`benchmarks`)

Tests all SQL operations across varying data sizes with automatic visual graphs.

#### Tokenization
- **simple_select**: Basic `SELECT * FROM users WHERE id = 123`
- **complex_select**: Complex query with JOIN, GROUP BY, HAVING, ORDER BY
- **insert**: INSERT statement parsing
- **update**: UPDATE statement parsing
- **delete**: DELETE statement parsing

**Purpose**: Measure SQL parsing overhead (typically < 1ms per query)

#### SELECT Operations
Tested with 100, 1K, and 10K row datasets:

- **full_scan**: `SELECT * FROM users` (no filtering)
- **where_filter**: `SELECT * FROM users WHERE age > 30 AND salary > 50000`
- **order_by**: `SELECT * FROM users ORDER BY salary DESC`
- **order_by_limit**: `SELECT * FROM users ORDER BY salary DESC LIMIT 10`

**Purpose**: Measure read performance, filtering overhead, and sorting efficiency.

**Key Insight**: LIMIT optimization should show minimal performance difference between 1K and 10K rows when limiting to 10 results.

#### Aggregation Operations
Tested with 100, 1K, and 10K row datasets:

- **count**: `SELECT COUNT(*) FROM users`
- **sum_avg**: `SELECT SUM(salary), AVG(age) FROM users`
- **group_by**: `SELECT department, COUNT(*), AVG(salary) FROM users GROUP BY department`
- **group_by_having**: `SELECT department, COUNT(*) as cnt FROM users GROUP BY department HAVING COUNT(*) > 5`

**Purpose**: Measure streaming aggregation performance and GROUP BY efficiency.

**Expected**: Linear O(n) performance for most operations. GROUP BY creates ~10 groups (departments).

#### JOIN Operations
Tested with 100, 500, and 1K row datasets:

- **inner_join**: `SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id`
- **left_join**: `SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id`

**Purpose**: Measure JOIN algorithm performance (nested loop implementation).

**Note**: Orders table is 2x size of users table for INNER JOIN, 0.5x for LEFT JOIN to test both matched and unmatched scenarios.

#### INSERT Operations
Tested with batch sizes of 1, 10, and 100 rows:

- **batch**: `INSERT INTO users (...) VALUES (...), (...), ...`

**Purpose**: Measure INSERT throughput and batch optimization.

**Key Insight**: Batch operations should show improved per-row performance due to single file open/lock/close cycle.

#### UPDATE Operations
Tested with 100, 1K, and 10K row datasets:

- **single_row**: `UPDATE users SET salary = 60000 WHERE id = 50`
- **multiple_rows**: `UPDATE users SET salary = salary * 1.1 WHERE age > 30`

**Purpose**: Measure update performance with different selectivity.

**Note**: Updates require full file read + rewrite (not in-place modification).

#### DELETE Operations
Tested with 100, 1K, and 10K row datasets:

- **single_row**: `DELETE FROM users WHERE id = 50`
- **multiple_rows**: `DELETE FROM users WHERE age < 30`

**Purpose**: Measure delete performance with file rewrite.

**Note**: DELETEs also require full file read + filtered rewrite.

#### UPSERT Operations
Tested with 100 and 1K row datasets:

- **insert_new**: UPSERT a row with non-existent key
- **update_existing**: UPSERT a row with existing key

**Purpose**: Measure key lookup performance and update vs insert paths.

**Note**: UPSERT requires linear scan to find key, then update or append.

### 2. Concurrency & File Locking (`concurrency_benchmark`)

Tests file locking overhead and concurrent access patterns.

#### Concurrent Reads
Tested with 2, 4, and 8 threads:

- **threads**: Multiple threads reading same file simultaneously

**Purpose**: Measure shared lock overhead and read scalability.

**Expected**: Near-linear scaling (reads don't block each other with shared locks).

#### Sequential Writes
Tested with 5, 10, and 20 operations:

- **operations**: Sequential UPDATE operations (each requires exclusive lock)

**Purpose**: Measure exclusive lock acquisition overhead.

**Note**: Operations are sequential (not concurrent) as exclusive locks serialize writes.

#### Concurrent Inserts
Tested with 5 and 10 operations:

- **operations**: Sequential INSERT operations

**Purpose**: Measure INSERT locking overhead (append-only operations).

#### UPSERT Locking
Tested with 100, 500, and 1K row tables:

- **table_size**: UPSERT operation requiring read + write lock

**Purpose**: Measure full-table scan + update overhead.

#### DELETE Locking
Tested with 100, 500, and 1K row tables:

- **table_size**: DELETE operation requiring full file rewrite

**Purpose**: Measure file rewrite performance with locking.

#### Mixed Workload
Tested with 100 and 500 row tables:

- **table_size**: Sequence of SELECT → UPDATE → SELECT → INSERT

**Purpose**: Measure lock contention in realistic read/write mix.

**Key Insight**: Should see minimal overhead from lock transitions.

## Interpreting Results

### Criterion Output

Criterion generates detailed statistics and graphs:

- **Mean**: Average execution time
- **Std Dev**: Standard deviation (consistency measure)
- **Median**: Middle value (less affected by outliers)
- **MAD**: Median Absolute Deviation (robust consistency measure)

### HTML Reports

After running `./run_benchmarks.sh`, open:

```
target/criterion/report/index.html
```

This provides:

- **Violin plots**: Distribution of execution times
- **Line charts**: Performance across different data sizes
- **Comparison tables**: Before/after comparisons for detecting regressions
- **Statistical analysis**: Outlier detection, confidence intervals

### Performance Targets

Based on streaming architecture and zero-copy parsing:

| Operation | Target (1K rows) | Target (10K rows) |
|-----------|------------------|-------------------|
| SELECT full scan | < 10ms | < 100ms |
| SELECT with WHERE | < 15ms | < 150ms |
| GROUP BY (10 groups) | < 20ms | < 200ms |
| INNER JOIN (1K x 2K) | < 50ms | - |
| INSERT (single) | < 5ms | - |
| UPDATE (single row) | < 50ms | < 500ms |
| DELETE (single row) | < 50ms | < 500ms |
| UPSERT | < 50ms | < 500ms |

**Note**: Write operations (INSERT/UPDATE/DELETE/UPSERT) require file rewrites, hence higher latency. This is a trade-off for simplicity and crash safety.

## Baseline Comparison

To establish a baseline for comparison:

```bash
# Run benchmarks and save baseline
./run_benchmarks.sh
cargo bench --bench benchmarks -- --save-baseline main

# After making changes, compare
cargo bench --bench benchmarks -- --baseline main

# Criterion will show performance deltas (faster/slower)
```

## Continuous Performance Monitoring

For CI/CD integration:

```bash
# Run benchmarks without reports (faster)
cargo bench --bench benchmarks --no-plot

# Save results for trend analysis
cargo bench --bench benchmarks -- --save-baseline "v$(cargo pkgid | cut -d'#' -f2)"
```

## Scaling Characteristics

Expected algorithmic complexity:

| Operation | Complexity | Notes |
|-----------|------------|-------|
| SELECT (no ORDER BY) | O(n) | Streaming, one pass |
| SELECT with ORDER BY | O(n log n) | Must materialize and sort |
| WHERE filtering | O(n) | Predicate eval per row |
| GROUP BY | O(n) | HashMap aggregation |
| INNER JOIN | O(n * m) | Nested loop (no indexes) |
| INSERT | O(1) | Append-only |
| UPDATE | O(n) | Read all + rewrite |
| DELETE | O(n) | Read all + filter + rewrite |
| UPSERT | O(n) | Linear key scan + write |

## Limitations

Current benchmark suite does NOT test:

- ❌ Multi-process concurrent access (only multi-threaded)
- ❌ Network latency (MCP server over stdio)
- ❌ Very large files (> 100K rows)
- ❌ Complex JOIN scenarios (3+ tables)
- ❌ Subqueries or CTEs (not implemented yet)
- ❌ Memory usage profiling

## Adding New Benchmarks

To add a new benchmark:

1. **Edit `benches/benchmarks.rs` or `benches/concurrency_benchmark.rs`**:

```rust
fn bench_my_operation(c: &mut Criterion) {
    let mut group = c.benchmark_group("my_operation");

    group.bench_function("test_case", |b| {
        b.iter(|| {
            // Your code here
        })
    });

    group.finish();
}

// Add to criterion_group!
criterion_group!(benches, ..., bench_my_operation);
```

2. **Run the benchmark**:

```bash
cargo bench --bench benchmarks my_operation
```

3. **View results**: Open `target/criterion/my_operation/report/index.html`

## Troubleshooting

### Benchmarks Taking Too Long

Reduce iterations or data sizes:

```rust
group.sample_size(10);  // Default is 100
group.measurement_time(Duration::from_secs(5));  // Default is 5s
```

### Inconsistent Results

Ensure system is idle:

```bash
# Close other applications
# Disable CPU frequency scaling
sudo cpupower frequency-set --governor performance
```

### Criterion Errors

Clean and rebuild:

```bash
cargo clean
cargo build --release
./run_benchmarks.sh
```

## Performance Optimization Guide

Based on benchmark results, prioritize:

1. **Hot paths**: Focus on operations shown to be slow
2. **Algorithmic improvements**: Better than micro-optimizations
3. **Memory allocations**: Reduce clones, use streaming
4. **I/O efficiency**: Minimize file operations

Use benchmarks to validate optimizations:

```bash
# Before optimization
cargo bench --bench benchmarks select -- --save-baseline before

# Make changes...

# After optimization (compare automatically)
cargo bench --bench benchmarks select -- --baseline before
```

## Contributing

When submitting performance improvements:

1. Run full benchmark suite
2. Include before/after comparison
3. Document algorithmic changes
4. Consider memory usage (not just speed)

## License

Same as DDB v2: Creative Commons Attribution-Noncommercial-Share Alike (CC-BY-NC-SA-4.0)
