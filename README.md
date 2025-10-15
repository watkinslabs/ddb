# DDB-Rust

A high-performance, secure Rust implementation of DDB (Delimited Database) - a serviceless SQL interface for flat files.

## Project Status

This is an **in-progress** Rust port of the Python DDB project. The goal is to provide better performance, memory safety, and security while maintaining feature parity with the original.

### Current Implementation Status

✅ **Completed:**
- Project structure with Cargo workspace
- Error handling with `thiserror`
- SQL tokenizer/lexer with `nom` parser
- Token types for all SQL keywords
- Configuration structures (Database, Table, Column)
- File I/O module with file locking (`fs2`)
- Streaming line reader for memory-efficient processing
- AST (Abstract Syntax Tree) definitions
- CLI interface with `clap`
- Benchmark suite with `criterion`

🚧 **In Progress:**
- SQL parser (tokens → AST)
- Query execution engine

📋 **Planned:**
- WHERE clause evaluation
- SELECT query implementation
- INSERT/UPDATE/DELETE operations
- SQL functions (database(), datetime(), count(), etc.)
- Output formatters (JSON, YAML, XML, terminal tables)
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- ORDER BY, LIMIT, DISTINCT operations

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
- **6 Tools**: execute_query, list_tables, describe_table, query_file, get_functions, validate_query
- **Dynamic Resources**: Configuration, table schemas, and SQL function documentation
- **7 Prompts**: Intelligent templates for common SQL operations

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
│   ├── server.rs       # MCP server implementation
│   ├── tools.rs        # MCP tools (6 tools)
│   ├── resources.rs    # MCP resources
│   └── prompts.rs      # MCP prompts (7 prompts)
└── bin/
    ├── ddb.rs          # CLI binary
    └── ddb_mcp.rs      # MCP server binary
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

This is an active development project. Key areas that need work:

1. **Parser** - Convert token stream to AST
2. **Query Executor** - Implement SELECT with WHERE/ORDER BY/LIMIT
3. **Write Operations** - INSERT, UPDATE, DELETE with file locking
4. **Functions** - Implement SQL functions
5. **Output** - Terminal tables, JSON, YAML, XML formatters
6. **Tests** - Comprehensive unit and integration tests

## License

Creative Commons Attribution-Noncommercial-Share Alike (same as Python DDB)

## Acknowledgments

Based on the original Python DDB by Charles Watkins (chris17453@gmail.com)
