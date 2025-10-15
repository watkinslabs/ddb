# DDB MCP Server

Model Context Protocol (MCP) server for DDB - A serviceless SQL interface for flat files.

## Overview

The DDB MCP Server exposes DDB's functionality through the Model Context Protocol, enabling LLMs and AI assistants to:
- Execute SQL queries against CSV and delimited files
- Explore table schemas and metadata
- Access comprehensive SQL function documentation
- Generate SQL queries using intelligent prompts

## Features

### Tools (3 available)

1. **execute_query** - Execute SQL queries (read & write operations)
   - **SELECT** statements with JOIN, GROUP BY, HAVING support
   - **INSERT** - Insert new rows into tables
   - **UPDATE** - Modify existing rows with WHERE conditions
   - **DELETE** - Remove rows with WHERE conditions
   - **UPSERT** - Insert or update rows based on key column
   - Support for WHERE clauses, ORDER BY, LIMIT, DISTINCT
   - Multiple output formats (JSON, YAML, CSV, table)
   - Full access to 101+ SQL functions
   - File locking for safe concurrent operations

2. **list_tables** - List all configured tables
   - Shows all tables from the DDB catalog
   - Returns table names from schema directory

3. **describe_table** - Get detailed table schema
   - Column names, types, and nullability
   - File location and delimiter information
   - Database and table metadata

### Resources (2 types)

- **ddb://config** - Current DDB configuration
- **ddb://tables/{table}/schema** - Schema for each configured table (dynamic)

### Prompts (2 available)

Intelligent prompt templates for common SQL operations:

1. **query_data** - Generate SELECT queries
2. **analyze_data** - Create aggregation queries (COUNT, SUM, AVG, etc.)

## Installation

### Build from Source

```bash
# Build (MCP included by default)
cargo build --release

# The binary is at:
./target/release/ddb
```

### Configuration

DDB uses configuration files located in `~/.ddb/` or your specified config directory:

```
~/.ddb/
├── ddb.yaml              # Main configuration
└── schemas/
    ├── users.yaml        # Table definition
    ├── transactions.yaml
    └── ...
```

Example `ddb.yaml`:
```yaml
default_database: main
schema_dir: ~/.ddb/schemas
default_delimiter: ','
data_starts_on: 0
comment_char: '#'
default_output_format: json
```

Example table schema (`schemas/users.yaml`):
```yaml
name: users
database: main
data_file: /path/to/users.csv
field_delimiter: ','
data_starts_on: 1  # Skip header row
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
```

## Usage with Claude Desktop

Add to your Claude Desktop configuration (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "ddb": {
      "command": "/path/to/ddb/target/release/ddb",
      "args": ["--mcp"],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

## Usage Examples

### SELECT with JOIN and GROUP BY

Using the `execute_query` tool:

```json
{
  "query": "SELECT u.name, COUNT(o.id) as order_count FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING order_count > 5",
  "output_format": "json"
}
```

### INSERT Data

```json
{
  "query": "INSERT INTO users (id, name, email, age) VALUES (101, 'John Doe', 'john@example.com', 30)"
}
```

### UPDATE Records

```json
{
  "query": "UPDATE users SET age = 31 WHERE name = 'John Doe'"
}
```

### DELETE Records

```json
{
  "query": "DELETE FROM users WHERE age < 18"
}
```

### UPSERT (Insert or Update)

```json
{
  "query": "UPSERT INTO users (id, name, email, age) VALUES (101, 'John Doe', 'newemail@example.com', 31) ON id"
}
```

### Analyze Data

Using the `analyze_data` prompt:

```json
{
  "table": "transactions",
  "metric": "avg",
  "column": "amount"
}
```

Returns a generated query:
```sql
SELECT AVG(amount) as average FROM transactions
```


## SQL Capabilities

### Supported SQL Features

- **SELECT statements** with full support for:
  - Column selection (wildcards, specific columns, aliases)
  - WHERE clauses (complex conditions with AND/OR)
  - **JOIN operations** (INNER, LEFT, RIGHT, FULL OUTER)
  - **GROUP BY** with aggregate functions
  - **HAVING** clause for filtered aggregations
  - ORDER BY (ASC/DESC, multiple columns)
  - LIMIT and OFFSET
  - DISTINCT

- **Data Modification Operations**:
  - **INSERT** - Add new rows to tables
  - **UPDATE** - Modify existing rows with WHERE conditions
  - **DELETE** - Remove rows with WHERE conditions
  - **UPSERT** - Insert or update based on key column (INSERT ON DUPLICATE KEY UPDATE pattern)

- **101+ SQL Functions** across categories:
  - Math: ABS, ROUND, SQRT, POW, MOD, etc.
  - String: CONCAT, UPPER, LOWER, TRIM, SUBSTR, etc.
  - Date/Time: NOW, DATEDIFF, DATEADD, YEAR, MONTH, etc.
  - Aggregate: COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE, etc.
  - Conditional: IF, IFNULL, COALESCE, CASE, etc.
  - Utility: BASE64, HASH, REGEXP, SPLIT_PART, etc.

### Coming Soon

- CREATE TABLE, DROP TABLE
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- Subqueries and CTEs (Common Table Expressions)
- Window functions

## Performance

DDB includes 4 major performance optimizations:

1. **Heap-based LIMIT** - 7-11% faster ORDER BY LIMIT queries
2. **Hash indexes for JOINs** - 100-1000x faster equality JOINs (O(n+m) vs O(n×m))
3. **Memory-mapped I/O** - 2-3x faster reads for files >= 10MB
4. **Parallel aggregations** - 2-4x faster SUM/AVG/STDDEV on multi-core systems

**Architecture Features:**
- **Streaming** - Processes files line-by-line, minimal memory usage
- **Zero-copy parsing** - Efficient tokenization with nom parser
- **Compiled binary** - Native performance with LTO optimizations

**Typical Performance:**
- Tokenization: ~496ns for simple SELECT (~2M queries/sec)
- Full table scan: ~1.2M rows/sec throughput
- Aggregations: ~1.9M rows/sec (COUNT/SUM/AVG)
- Batch inserts: ~2.8M rows/sec (100-row batches)

See [BENCHMARKS.md](BENCHMARKS.md) for detailed performance metrics with visual graphs.

## Debugging

Enable detailed logging:

```bash
RUST_LOG=ddb_mcp=debug ./target/release/ddb --mcp
```

Test queries using the `execute_query` tool to validate SQL syntax.

## Architecture

```
DDB MCP Server
├── Tools (3)
│   ├── execute_query
│   ├── list_tables
│   └── describe_table
├── Resources (2 types)
│   ├── Configuration
│   └── Table schemas (dynamic)
└── Prompts (2)
    ├── Query generation
    └── Data analysis
```

## Error Handling

The server provides detailed error messages for:
- Invalid SQL syntax
- Missing tables or files
- Type conversion errors
- File access issues
- Configuration problems

All errors are returned in a structured format with context.

## Security

- **Write operations supported** - INSERT, UPDATE, DELETE, UPSERT modify files directly
- **File locking** - Exclusive locks prevent concurrent write conflicts
- **No SQL injection** - All queries parsed and validated before execution
- **File system isolation** - Operations restricted to configured table files
- **No network access** - Pure local file processing
- **Atomic operations** - Updates rewrite entire files to ensure consistency

**Important**: Write operations (INSERT, UPDATE, DELETE, UPSERT) modify your data files. Ensure you have backups or use version control for important data.

## Contributing

The MCP server is part of the DDB v2 project. Contributions welcome!

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Creative Commons Attribution-Noncommercial-Share Alike (CC-BY-NC-SA-4.0)

## Support

- GitHub Issues: https://github.com/chris17453/ddb/issues
- Documentation: [README.md](README.md)
- Function Reference: [FUNCTIONS.md](FUNCTIONS.md)

## Version

Current version: 2.0.0 (DDB v2)

MCP Protocol: 2024-11-05
