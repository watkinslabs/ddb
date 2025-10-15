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

1. **execute_query** - Execute SQL queries
   - Run SELECT statements against configured tables
   - Support for WHERE clauses, ORDER BY, LIMIT, DISTINCT
   - Multiple output formats (JSON, YAML, CSV, table)
   - Full access to 101+ SQL functions

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
      "command": "/path/to/ddb-rust/target/release/ddb",
      "args": ["--mcp"],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

## Usage Examples

### Execute a Query

Using the `execute_query` tool:

```json
{
  "query": "SELECT name, email FROM users WHERE age > 25 ORDER BY name LIMIT 10",
  "output_format": "json"
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
  - ORDER BY (ASC/DESC, multiple columns)
  - LIMIT and OFFSET
  - DISTINCT
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX, etc.)

- **101+ SQL Functions** across categories:
  - Math: ABS, ROUND, SQRT, POW, MOD, etc.
  - String: CONCAT, UPPER, LOWER, TRIM, SUBSTR, etc.
  - Date/Time: NOW, DATEDIFF, DATEADD, YEAR, MONTH, etc.
  - Aggregate: COUNT, SUM, AVG, MIN, MAX, STDDEV, etc.
  - Conditional: IF, IFNULL, COALESCE, CASE, etc.
  - Utility: BASE64, HASH, REGEXP, SPLIT_PART, etc.

### Coming Soon

- INSERT, UPDATE, DELETE operations
- CREATE TABLE, DROP TABLE
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- JOIN operations
- GROUP BY with HAVING

## Performance

- **Streaming architecture** - Processes files line-by-line, minimal memory usage
- **Zero-copy parsing** - Efficient tokenization with nom parser
- **Memory-mapped I/O** - Optional mmap for large file performance
- **Compiled binary** - Native performance with LTO optimizations

Typical performance:
- Tokenization: < 1ms for most queries
- SELECT on 100K rows: ~100-500ms (depending on complexity)
- Aggregations: Linear O(n) with streaming

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

- **Read-only by default** - SELECT operations are safe
- **No SQL injection** - Parsed and validated before execution
- **File system isolation** - Can be configured to restrict file access
- **No network access** - Pure local file processing

## Contributing

The MCP server is part of the ddb-rust project. Contributions welcome!

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Creative Commons Attribution-Noncommercial-Share Alike (CC-BY-NC-SA-4.0)

## Support

- GitHub Issues: https://github.com/chris17453/ddb/issues
- Documentation: [README.md](README.md)
- Function Reference: [FUNCTIONS.md](FUNCTIONS.md)

## Version

Current version: 0.1.0

MCP Protocol: 2024-11-05
