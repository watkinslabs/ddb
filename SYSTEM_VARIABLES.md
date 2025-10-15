# System Variables in DDB-Rust

## Overview

DDB-Rust supports system variables similar to MSSQL's `@@VARIABLE` syntax. These special variables provide information about the database system, runtime state, and configuration.

## Syntax

System variables are prefixed with `@@` and can be used anywhere a column reference or expression is allowed:

```sql
SELECT @@VERSION
SELECT @@DB_NAME, @@DB_TYPE FROM mytable
SELECT * FROM mytable WHERE @@VERSION = '0.1.0'
```

## Available System Variables

### Static Variables (Compile-time constants)

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `@@VERSION` | DDB-Rust version number | `0.1.0` |
| `@@DB_NAME` | Database name | `DDB-Rust` |
| `@@DB_TYPE` | Database type description | `Flat File Database` |

### Runtime Variables (Query execution state)

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `@@ROWS_SCANNED` | Number of rows scanned in last query | `0` |
| `@@ROWS_RETURNED` | Number of rows returned in last query | `0` |
| `@@LAST_ERROR` | Last error message (if any) | `NULL` |

**Note**: Runtime variables are currently read-only and not yet updated by query execution. This will be implemented in a future version.

## Examples

### Query System Information

```sql
SELECT @@VERSION AS version, @@DB_TYPE AS database_type
FROM benchmark_data
LIMIT 1;
```

Output:
```
+---------+--------------------+
| version | database_type      |
+---------+--------------------+
| 0.1.0   | Flat File Database |
+---------+--------------------+
```

### Combine with Regular Columns

```sql
SELECT @@VERSION, customer_name, order_id
FROM benchmark_data
WHERE region = 'West'
LIMIT 3;
```

### Use in WHERE Clause

```sql
SELECT *
FROM benchmark_data
WHERE @@DB_NAME = 'DDB-Rust' AND status = 'pending'
LIMIT 5;
```

## Implementation Details

### Architecture

1. **Lexer**: The tokenizer recognizes `@@` prefix as part of identifiers
2. **Parser**: System variables are parsed as `Expression::SystemVariable(name)`
3. **Evaluator**: A `SystemVariables` struct maintains all available system variables
4. **Executor**: When projecting SELECT columns, the executor checks for `@@` prefix and evaluates system variables

### Code Structure

- `src/engine/system_vars.rs` - System variables registry
- `src/parser/ast.rs` - `Expression::SystemVariable` variant
- `src/lexer/tokenizer.rs` - `@@` identifier support
- `src/engine/evaluator.rs` - System variable evaluation
- `src/engine/executor.rs` - System variable projection

### Adding New System Variables

To add a new system variable:

1. Open `src/engine/system_vars.rs`
2. Add the variable in `SystemVariables::new()`:

```rust
variables.insert("MY_VAR".to_string(), Value::String("value".to_string()));
```

3. Rebuild: `cargo build --release`

## Comparison with MSSQL

DDB-Rust system variables are inspired by MSSQL's `@@` variables:

| MSSQL Variable | DDB-Rust Equivalent | Notes |
|----------------|---------------------|-------|
| `@@VERSION` | `@@VERSION` | Supported |
| `@@SERVERNAME` | `@@DB_NAME` | Similar concept |
| `@@ROWCOUNT` | `@@ROWS_RETURNED` | Planned (not yet implemented) |
| `@@ERROR` | `@@LAST_ERROR` | Planned (not yet implemented) |

Unlike MSSQL, DDB-Rust variables are case-insensitive:
- `@@VERSION` = `@@version` = `@@Version`

## Future Enhancements

Planned improvements:

1. **Dynamic Runtime Variables**: Update `@@ROWS_SCANNED` and `@@ROWS_RETURNED` after each query
2. **Configuration Variables**: Access config settings via system variables (e.g., `@@DELIMITER`, `@@DEFAULT_OUTPUT`)
3. **Performance Metrics**: Add variables for query timing and memory usage
4. **Session Variables**: Support user-defined session-level variables

## Error Handling

Attempting to access an undefined system variable returns an error:

```sql
SELECT @@UNKNOWN_VAR FROM mytable;
```

Error:
```
Unknown system variable: @@UNKNOWN_VAR
```

## Testing

Test system variables:

```bash
# Test basic usage
./target/release/ddb --query "SELECT @@VERSION, @@DB_NAME FROM benchmark_data LIMIT 1"

# Test in WHERE clause
./target/release/ddb --query "SELECT * FROM benchmark_data WHERE @@VERSION = '0.1.0' LIMIT 5"

# Test multiple variables
./target/release/ddb --query "SELECT @@VERSION, @@DB_NAME, @@DB_TYPE FROM benchmark_data LIMIT 1"
```

---

**Last Updated**: 2025-10-14
**DDB-Rust Version**: 0.1.0
