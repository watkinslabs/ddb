# DDB v2

**A high-performance, secure SQL interface for CSV and delimited files.**

Query flat files with full SQL power - no database server required. Built in Rust for maximum performance and safety.

```sql
SELECT u.name, COUNT(o.id) as orders, SUM(o.total) as revenue
FROM users.csv u
INNER JOIN orders.csv o ON u.id = o.user_id
GROUP BY u.name
HAVING revenue > 10000
ORDER BY revenue DESC
```

## Why DDB v2?

- 🚀 **Fast** - Zero-copy parsing, streaming architecture, compiled performance
- 🔒 **Safe** - Memory-safe Rust, file locking, no SQL injection
- 💪 **Powerful** - Full SQL: JOIN, GROUP BY, HAVING, UPSERT, 101+ functions
- 🤖 **AI-Ready** - Built-in MCP server for Claude and other AI assistants
- 📦 **Portable** - Single binary, no dependencies, works anywhere
- 🎯 **Simple** - No database server, no setup complexity

This is **DDB v2** - a complete rewrite in Rust. Provides better performance, memory safety, and security while maintaining the core functionality of querying delimited files with SQL.

## Use Cases

**Perfect for:**
- 📊 Ad-hoc analysis of CSV/TSV exports
- 🔄 ETL data transformations without setting up a database
- 🧪 Testing and prototyping with flat file data
- 📝 Log file analysis and reporting
- 🤖 AI assistants querying structured data files
- 🚀 Serverless data processing in containers/CI
- 💼 Data auditing and compliance checks

**Example scenarios:**
```bash
# Analyze web server logs
ddb --query "SELECT ip, COUNT(*) FROM access.log GROUP BY ip ORDER BY count DESC LIMIT 10"

# Join sales data from multiple CSV exports
ddb --query "
  SELECT r.region, SUM(s.amount) as total_sales
  FROM sales.csv s
  JOIN regions.csv r ON s.region_id = r.id
  GROUP BY r.region
"

# Update customer records in place
ddb --query "UPDATE customers.csv SET status = 'premium' WHERE total_purchases > 10000"
```

### Current Implementation Status

✅ **Fully Implemented (v0.1.0):**
- **SQL Query Support:**
  - Complete SELECT statement implementation with:
    - **JOIN operations** (INNER, LEFT, RIGHT, FULL OUTER)
    - **GROUP BY** with aggregate functions
    - **HAVING** clause for filtered aggregations
    - WHERE clause evaluation with complex conditions (AND, OR, comparison operators)
    - ORDER BY (ASC/DESC, multiple columns)
    - LIMIT and OFFSET
    - DISTINCT
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE)

- **Data Modification Operations:**
  - **INSERT** - Add new rows to tables
  - **UPDATE** - Modify existing rows with WHERE conditions
  - **DELETE** - Remove rows with WHERE conditions
  - **UPSERT** - Insert or update based on key column
  - File locking for safe concurrent operations

- **101+ SQL Functions:**
  - Math: ABS, ROUND, SQRT, POW, MOD, CEIL, FLOOR, etc.
  - String: CONCAT, UPPER, LOWER, TRIM, SUBSTR, LENGTH, LIKE, etc.
  - Date/Time: NOW, DATEDIFF, DATEADD, YEAR, MONTH, DAY, etc.
  - Conditional: IF, IFNULL, COALESCE, CASE, NULLIF
  - Utility: BASE64, HASH, REGEXP, SPLIT_PART, UUID, etc.

- **System Variables:**
  - MSSQL-style `@@VARIABLE` syntax
  - Variables: `@@VERSION`, `@@DB_NAME`, `@@DB_TYPE`, etc.

- **Output Formats:**
  - JSON, YAML, CSV, terminal tables

- **Performance:**
  - **4 Major Optimizations:**
    1. **Heap-based LIMIT** - 7-11% faster ORDER BY LIMIT queries
    2. **Hash indexes for JOINs** - 100-1000x faster equality JOINs (O(n+m) vs O(n×m))
    3. **Memory-mapped I/O** - 2-3x faster reads for large files (>=10MB)
    4. **Parallel aggregations** - 2-4x faster SUM/AVG/STDDEV on multi-core (>=1K rows)
  - Streaming architecture (memory-efficient)
  - Zero-copy parsing with `nom`
  - LIKE pattern optimization (19.6x speedup)
  - See [BENCHMARKS.md](BENCHMARKS.md) for detailed performance metrics with graphs

- **MCP Server:**
  - Model Context Protocol integration for AI assistants
  - 3 tools (with full CRUD support), 2 resources, 2 prompts
  - Activated with `--mcp` flag

📋 **Not Yet Implemented:**
- CREATE TABLE, DROP TABLE statements
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- Subqueries and CTEs (Common Table Expressions)
- Window functions

## Quick Start

```bash
# 1. Build DDB
cargo build --release

# 2. Create configuration directory
mkdir -p ~/.ddb/schemas

# 3. Create main config file
cat > ~/.ddb/ddb.yaml <<EOF
default_database: main
schema_dir: ~/.ddb/schemas
default_delimiter: ','
data_starts_on: 0
default_output_format: json
EOF

# 4. Create a table schema (example: users.yaml)
cat > ~/.ddb/schemas/users.yaml <<EOF
name: users
database: main
data_file: /path/to/your/users.csv
field_delimiter: ','
data_starts_on: 1
columns:
  - name: id
    type: Integer
    nullable: false
  - name: name
    type: String
    nullable: false
  - name: email
    type: String
    nullable: true
EOF

# 5. Query your data!
./target/release/ddb --query "SELECT * FROM users WHERE id = 1"

# 6. Use with JOIN and aggregation
./target/release/ddb --query "
  SELECT u.name, COUNT(o.id) as order_count
  FROM users u
  INNER JOIN orders o ON u.id = o.user_id
  GROUP BY u.name
  HAVING order_count > 5
"
```

## Features

### Security First
- Memory-safe Rust implementation
- No SQL injection vulnerabilities (parameterized queries)
- Proper error handling throughout
- File locking prevents concurrent write conflicts

### Performance
- Zero-copy parsing with `nom`
- Streaming/iterator-based file reading (low memory footprint)
- Compiled binary with LTO and optimizations
- Memory-mapped file support for large files

### Compatibility
- Same configuration format as Python DDB
- Similar SQL dialect support
- Multiple output formats

## Building

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Usage

### CLI Usage

```bash
# Execute a SQL query
./target/release/ddb --query "SELECT * FROM users WHERE id = 123"

# With debug output
./target/release/ddb --query "SELECT * FROM test" --debug

# Specify config directory
./target/release/ddb --query "SELECT * FROM test" --config ./config

# Show version
./target/release/ddb version
```

### SQL Examples

**SELECT with JOIN:**
```sql
SELECT u.name, o.order_id, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE o.total > 100
ORDER BY o.total DESC
```

**GROUP BY with HAVING:**
```sql
SELECT category, COUNT(*) as total, AVG(price) as avg_price
FROM products
GROUP BY category
HAVING COUNT(*) > 5 AND AVG(price) > 50
ORDER BY total DESC
```

**INSERT data:**
```sql
INSERT INTO users (id, name, email, age)
VALUES (101, 'John Doe', 'john@example.com', 30)
```

**UPDATE records:**
```sql
UPDATE users
SET age = 31, email = 'newemail@example.com'
WHERE name = 'John Doe'
```

**DELETE records:**
```sql
DELETE FROM users WHERE age < 18
```

**UPSERT (insert or update):**
```sql
UPSERT INTO users (id, name, email, age)
VALUES (101, 'John Doe', 'updated@example.com', 32)
ON id
```

**Advanced aggregation:**
```sql
SELECT
  department,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary,
  MIN(salary) as min_salary,
  MAX(salary) as max_salary,
  STDDEV(salary) as salary_stddev
FROM employees
GROUP BY department
HAVING AVG(salary) > 50000
ORDER BY avg_salary DESC
```

### MCP Server (AI Integration)

DDB includes an MCP (Model Context Protocol) server for seamless integration with AI assistants like Claude.

```bash
# Build (MCP included by default)
cargo build --release

# Run the MCP server
./target/release/ddb --mcp
```

The MCP server provides:
- **3 Tools**:
  - `execute_query` - Full CRUD support (SELECT with JOIN/GROUP BY/HAVING, INSERT, UPDATE, DELETE, UPSERT)
  - `list_tables` - List all configured tables
  - `describe_table` - Get table schema and metadata
- **2 Resource Types**: Configuration and table schemas
- **2 Prompts**: Query generation and data analysis templates

See [MCP_SERVER.md](MCP_SERVER.md) for complete documentation.

## Configuration

DDB uses YAML configuration files located in `~/.ddb/` (or a custom directory via `--config`).

### Main Configuration (`~/.ddb/ddb.yaml`)

```yaml
default_database: main
schema_dir: ~/.ddb/schemas
default_delimiter: ','
data_starts_on: 0         # Line where data starts (0 = after header)
comment_char: '#'          # Optional: lines starting with this are ignored
default_output_format: json
```

### Table Schema (`~/.ddb/schemas/users.yaml`)

```yaml
name: users
database: main
data_file: /path/to/users.csv
field_delimiter: ','
data_starts_on: 1         # Skip header row
columns:
  - name: id
    type: Integer
    nullable: false
  - name: name
    type: String
    nullable: false
  - name: email
    type: String
    nullable: true
  - name: age
    type: Integer
    nullable: true
  - name: created_at
    type: DateTime
    nullable: false
```

### Supported Data Types

- `Integer` - 64-bit signed integers
- `Float` - 64-bit floating point
- `String` - UTF-8 text
- `Boolean` - true/false
- `Date` - Date only (YYYY-MM-DD)
- `DateTime` - Date and time
- `Time` - Time only (HH:MM:SS)

### Directory Structure

```
~/.ddb/
├── ddb.yaml              # Main configuration
└── schemas/
    ├── users.yaml        # Table definition
    ├── orders.yaml       # Table definition
    ├── products.yaml     # Table definition
    └── ...
```

## Architecture

```
src/
├── lib.rs              # Main library entry point
├── error.rs            # Error types and Result alias
├── lexer/              # Tokenization
│   ├── mod.rs
│   ├── types.rs        # Token types
│   └── tokenizer.rs    # SQL tokenizer
├── parser/             # SQL parsing
│   ├── mod.rs
│   └── ast.rs          # Abstract syntax tree
├── config/             # Configuration management
│   └── mod.rs          # Database, Table, Column structs
├── file_io/            # File operations
│   ├── locking.rs      # File locking for concurrency
│   └── reader.rs       # Streaming line reader
├── engine/             # Query execution
├── methods/            # SQL operations (SELECT, INSERT, etc.)
├── functions/          # SQL functions
├── output/             # Output formatters
├── mcp/                # Model Context Protocol server
│   ├── mod.rs
│   ├── server.rs       # MCP server implementation (JSON-RPC over stdio)
│   ├── tools.rs        # MCP tools (3 tools)
│   ├── resources.rs    # MCP resources (2 types)
│   └── prompts.rs      # MCP prompts (2 prompts)
└── bin/
    └── ddb.rs          # CLI binary (includes --mcp mode)
```

## Design Principles

1. **Memory Efficiency**: Stream data line-by-line, never load entire files into memory
2. **Safety**: Leverage Rust's type system to prevent bugs at compile time
3. **Performance**: Use zero-copy parsing and avoid unnecessary allocations
4. **Correctness**: Comprehensive testing and error handling
5. **Ergonomics**: Clean API and good error messages

## Dependencies

- `nom` - Fast parser combinators for SQL tokenization
- `serde` - Serialization/deserialization
- `chrono` - Date/time functions
- `fs2` - File locking for concurrent access
- `memmap2` - Memory-mapped file I/O for large files
- `rayon` - Data parallelism for aggregations
- `clap` - CLI argument parsing
- `thiserror`/`anyhow` - Error handling
- `regex` - Pattern matching for LIKE operations
- `uuid` - UUID generation

## Benchmarks

Run benchmarks to validate performance:

```bash
# Run all benchmarks
cargo bench

# View detailed results
open target/criterion/report/index.html
```

**Performance Highlights:**
- **Tokenization**: ~0.5µs (0.0000005 sec) for simple SELECT (~2M queries/sec)
- **Full table scan**: ~1.2M rows/sec throughput
- **Aggregations**: ~1.9M rows/sec (COUNT/SUM/AVG)
- **JOINs**: 100-1000x faster with hash index optimization (O(n+m) vs O(n×m))
- **Batch inserts**: 36x faster per row than single inserts (~2.8M rows/sec for 100-row batches)

See [BENCHMARKS.md](BENCHMARKS.md) for comprehensive benchmark results with detailed performance graphs and human-readable time conversions.

## Contributing

Contributions welcome! Key areas for future development:

1. **Subqueries** - Nested SELECT statements
2. **CREATE/DROP TABLE** - DDL operations
3. **Transaction Support** - BEGIN, COMMIT, ROLLBACK
4. **Window Functions** - ROW_NUMBER, RANK, LAG, LEAD, etc.
5. **CTEs** - Common Table Expressions (WITH clause)
6. **Additional Tests** - More unit and integration test coverage
7. **Query Planning** - Cost-based optimizer for complex queries

## License

Creative Commons Attribution-Noncommercial-Share Alike (CC-BY-NC-SA-4.0)
