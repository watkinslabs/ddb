# DDB Default Configuration

This document explains the default settings used by DDB in different modes.

## Direct File Mode

When using DDB with the `--file` option, the following defaults are applied:

```bash
./target/release/ddb --query "SELECT * FROM table_name" --file data.csv
```

### File Parsing Defaults

| Setting | Default Value | CLI Override | Description |
|---------|--------------|--------------|-------------|
| **delimiter** | `,` (comma) | `--delimiter` or `-d` | Field delimiter character |
| **data_starts_on** | `0` | N/A | Line number where data starts (0 = first line) |
| **has_header** | `true` | N/A | Whether first line is a header |
| **comment_char** | `#` | N/A | Lines starting with this are ignored |
| **quote_char** | `"` | N/A | Character used to quote fields |
| **trim_whitespace** | `true` | N/A | Remove leading/trailing whitespace |
| **skip_empty_lines** | `true` | N/A | Ignore empty lines |
| **strip_quotes** | `true` | N/A | Remove quotes from quoted fields |
| **quoted_fields** | `false` | N/A | Whether fields are quoted |
| **ignore_comments** | `true` | N/A | Skip lines starting with comment_char |
| **ignore_errors** | `false` | N/A | Continue processing on errors |

### Output Defaults

| Setting | Default Value | CLI Override | Description |
|---------|--------------|--------------|-------------|
| **output_format** | `table` | `--output` or `-o` | Output format (table, json, yaml, csv, xml) |

### Example: Custom Delimiter

```bash
# Tab-delimited file
./target/release/ddb --query "SELECT * FROM data" --file data.tsv --delimiter $'\t'

# Pipe-delimited file
./target/release/ddb --query "SELECT * FROM data" --file data.txt --delimiter '|'

# Semicolon-delimited file
./target/release/ddb --query "SELECT * FROM data" --file data.csv --delimiter ';'
```

### Example: Output Formats

```bash
# Table output (default)
./target/release/ddb --query "SELECT * FROM data" --file data.csv --output table

# JSON output
./target/release/ddb --query "SELECT * FROM data" --file data.csv --output json

# YAML output
./target/release/ddb --query "SELECT * FROM data" --file data.csv --output yaml

# CSV output
./target/release/ddb --query "SELECT * FROM data" --file data.csv --output csv

# XML output
./target/release/ddb --query "SELECT * FROM data" --file data.csv --output xml
```

## Config-Based Mode

When using DDB with the `--config` option, settings are loaded from the configuration file:

```bash
./target/release/ddb --config path/to/config --query "SELECT * FROM table_name"
```

### Configuration File Example

```yaml
# config.yaml
default_database: main
schema_dir: ./schemas
default_delimiter: ','
data_starts_on: 1          # Skip header row
comment_char: '#'
default_output_format: table
trim_whitespace: true
ignore_comments: true
has_header: true
quote_char: '"'
skip_empty_lines: true
quoted_fields: false
strip_quotes: true
ignore_errors: false
```

### Schema Directory

The schema directory contains SQL CREATE TABLE statements that define table structures:

```sql
-- schemas/sales_data.sql
CREATE TABLE IF NOT EXISTS sales_data (
    order_id INTEGER NOT NULL,
    customer_name STRING NOT NULL,
    product STRING NOT NULL,
    quantity INTEGER NOT NULL,
    price FLOAT NOT NULL,
    order_date DATE NOT NULL,
    region STRING NOT NULL,
    status STRING NOT NULL
)
FILE '/path/to/data/sales_data.csv'
DELIMITER ','
DATA_STARTS_ON 1
COMMENT_CHAR '#'
QUOTE_CHAR '"';
```

## Configuration Precedence

DDB loads configuration in the following order (first found wins):

1. **`DDB_CONFIG` environment variable** - Path to config file
2. **`.ddb/config.yaml`** - Local directory config
3. **`~/.ddb/config.yaml`** - Home directory config
4. **Built-in defaults** - Hard-coded defaults as shown above

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DDB_CONFIG` | Path to configuration file | `export DDB_CONFIG=/path/to/config.yaml` |
| `DDB_SCHEMA_DIR` | Path to schema directory | `export DDB_SCHEMA_DIR=~/.ddb/schemas` |

## CLI Options

Run `./target/release/ddb --help` to see all available options:

```
Options:
  -q, --query <QUERY>          SQL query to execute
  -f, --file <FILE>            Data file path
  -d, --delimiter <DELIMITER>  Field delimiter (default: comma) [default: ,]
  -c, --config <CONFIG>        Database configuration directory
  -o, --output <OUTPUT>        Output format (json, yaml, csv, xml, table) [default: table]
      --debug                  Enable debug mode
      --mcp                    Start MCP (Model Context Protocol) server mode
  -h, --help                   Print help
  -V, --version                Print version
```

## Notes

- In **direct file mode** (`--file`), the table name in the query is ignored
- In **config-based mode** (`--config`), table names must match those defined in CREATE TABLE statements
- Column names are read from the CSV header row (when `has_header: true`)
- Type inference is automatic in direct file mode
- Explicit type definitions are used in config-based mode (from CREATE TABLE statements)
