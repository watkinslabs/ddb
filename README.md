# DDB

A high-performance, secure serviceless SQL interface for flat files.

## Project Status

This is **DDB v2** - a complete rewrite in Rust. The goal is to provide better performance, memory safety, and security while maintaining the core functionality of querying delimited files with SQL.

### Current Implementation Status

✅ **Fully Implemented (v0.1.0):**
- **SQL Query Support:**
  - Complete SELECT statement implementation
  - WHERE clause evaluation with complex conditions (AND, OR, comparison operators)
  - ORDER BY (ASC/DESC, multiple columns)
  - LIMIT and OFFSET
  - DISTINCT
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE)

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
  - Streaming architecture (memory-efficient)
  - Zero-copy parsing with `nom`
  - LIKE pattern optimization (19.6x speedup)
  - 2M row queries in ~4 seconds

- **MCP Server:**
  - Model Context Protocol integration for AI assistants
  - 3 tools, 2 resources, 2 prompts
  - Activated with `--mcp` flag

📋 **Not Yet Implemented:**
- INSERT, UPDATE, DELETE operations
- CREATE TABLE, DROP TABLE statements
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- JOIN operations (INNER, LEFT, RIGHT, FULL)
- GROUP BY with HAVING clauses
- Subqueries

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

### MCP Server (AI Integration)

DDB includes an MCP (Model Context Protocol) server for seamless integration with AI assistants like Claude.

```bash
# Build (MCP included by default)
cargo build --release

# Run the MCP server
./target/release/ddb --mcp
```

The MCP server provides:
- **3 Tools**: execute_query, list_tables, describe_table
- **2 Resource Types**: Configuration and table schemas
- **2 Prompts**: Query generation and data analysis templates

See [MCP_SERVER.md](MCP_SERVER.md) for complete documentation.

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
- `clap` - CLI argument parsing
- `thiserror`/`anyhow` - Error handling
- `regex` - Pattern matching for LIKE operations
- `uuid` - UUID generation

## Benchmarks

Run benchmarks to compare performance:

```bash
cargo bench
```

Current tokenizer performance (SELECT query):
- ~X µs per query (TODO: add actual benchmark results)

## Contributing

Contributions welcome! Key areas for future development:

1. **Write Operations** - INSERT, UPDATE, DELETE with file locking
2. **JOIN Support** - INNER, LEFT, RIGHT, FULL JOIN operations
3. **GROUP BY** - Grouping with HAVING clauses
4. **Subqueries** - Nested SELECT statements
5. **CREATE/DROP TABLE** - DDL operations
6. **Transaction Support** - BEGIN, COMMIT, ROLLBACK
7. **Additional Tests** - More unit and integration test coverage

## License

Creative Commons Attribution-Noncommercial-Share Alike (CC-BY-NC-SA-4.0)
