# DDB Examples

This directory contains sample data and usage examples for DDB-Rust.

## Files

- **`sales_data.csv`** - Sample sales data with 20 orders
- **`schema.sql`** - CREATE TABLE statement and 20 example SQL queries
- **`WALKTHROUGH.md`** - Comprehensive guide with examples
- **`config/`** - Local configuration for testing DDB with table schemas
  - `ddb.yaml` - Main DDB configuration
  - `schemas/sales_data.sql` - CREATE TABLE statement for sales_data table

## Quick Start

### 1. Build DDB

```bash
cargo build --release
```

### 2. Choose Your Method

DDB supports **two ways** to query data:

#### Method A: Direct File Query (Simple)
Query a single file without any configuration:

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data" \
  --file examples/sales_data.csv
```

**Pros**: No setup, great for quick queries
**Cons**: Can't JOIN multiple tables

#### Method B: Config-Based Query (Advanced)
Use the local configuration for more advanced features:

```bash
./target/release/ddb \
  --config examples/config \
  --query "SELECT * FROM sales_data"
```

**Pros**: Supports JOINs, explicit types, multiple tables
**Cons**: Requires SQL schema files and configuration

The examples below show both methods!

### 3. Try Filtering

**Method A (Direct file):**
```bash
./target/release/ddb \
  --query "SELECT customer_name, product, price FROM sales_data WHERE price > 500" \
  --file examples/sales_data.csv
```

**Method B (Config-based):**
```bash
./target/release/ddb \
  --config examples/config \
  --query "SELECT customer_name, product, price FROM sales_data WHERE price > 500"
```

### 4. Sort and Limit

```bash
# Works with both methods - just swap --file for --config
./target/release/ddb \
  --query "SELECT * FROM sales_data ORDER BY price DESC LIMIT 5" \
  --file examples/sales_data.csv
```

### 5. Use Functions

```bash
# Functions work the same in both methods
./target/release/ddb \
  --query "SELECT order_id, UPPER(customer_name) as name, ROUND(price, 0) FROM sales_data" \
  --file examples/sales_data.csv
```

### 6. Output Formats

```bash
# JSON output
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West' LIMIT 2" \
  --file examples/sales_data.csv \
  --output json

# YAML output
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West' LIMIT 2" \
  --file examples/sales_data.csv \
  --output yaml

# CSV output
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West' LIMIT 2" \
  --file examples/sales_data.csv \
  --output csv

# XML output
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West' LIMIT 2" \
  --file examples/sales_data.csv \
  --output xml

# Table output (default)
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West' LIMIT 2" \
  --file examples/sales_data.csv \
  --output table
```

## Sample Queries

### Basic Selection
```sql
SELECT * FROM sales_data;
SELECT order_id, customer_name, price FROM sales_data;
```

### Filtering
```sql
SELECT * FROM sales_data WHERE price > 500;
SELECT * FROM sales_data WHERE status = 'shipped';
SELECT * FROM sales_data WHERE region = 'West' AND price > 100;
```

### Pattern Matching
```sql
SELECT * FROM sales_data WHERE product LIKE '%Laptop%';
SELECT * FROM sales_data WHERE customer_name LIKE 'J%';
```

### Sorting
```sql
SELECT * FROM sales_data ORDER BY price DESC;
SELECT * FROM sales_data ORDER BY order_date ASC, price DESC;
```

### Limiting Results
```sql
SELECT * FROM sales_data LIMIT 10;
SELECT * FROM sales_data LIMIT 5 OFFSET 10;
```

### Using Functions
```sql
SELECT UPPER(customer_name), ROUND(price, 0) FROM sales_data;
SELECT order_id, YEAR(order_date), MONTH(order_date) FROM sales_data;
```

## Configuration Directory

The `config/` directory contains a complete local setup for testing DDB:

```
config/
├── ddb.yaml                    # Main DDB configuration
└── schemas/
    └── sales_data.sql          # CREATE TABLE statement
```

This demonstrates the **configuration-based approach** which uses SQL CREATE TABLE statements for:
- **JOINs** between multiple tables
- **Explicit type definitions** and validation
- **Multiple tables** in the same database
- **MCP server** integration with AI assistants

The CREATE TABLE syntax:
```sql
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
FILE '/path/to/examples/sales_data.csv'
DELIMITER ','
DATA_STARTS_ON 1
COMMENT_CHAR '#';
```

To use it:
```bash
./target/release/ddb --config examples/config --query "YOUR_QUERY"
```

## Data Overview

The `sales_data.csv` contains:
- 20 orders
- Columns: order_id, customer_name, product, quantity, price, order_date, region, status
- Products: Laptop, Mouse, Keyboard, Monitor, Desk, Chair
- Regions: North, South, East, West
- Statuses: shipped, pending, processing, cancelled

## More Examples

See **`schema.sql`** for **20 ready-to-run example queries** including:
- Basic SELECT, WHERE, ORDER BY, LIMIT
- Aggregations with GROUP BY and HAVING
- String, math, and date functions
- Complex queries with multiple conditions
- All queries work with both direct file and config-based methods

See **`WALKTHROUGH.md`** for comprehensive examples including:
- Step-by-step query examples
- All 101 SQL functions
- Output format examples (JSON, YAML, CSV, Table)
- Custom delimiters
- Debug mode
- Performance tips
- Common patterns

## Need Help?

- Check `schema.sql` for 20 copy-paste ready queries
- Check `WALKTHROUGH.md` for detailed examples
- See `FUNCTIONS.md` in the project root for function reference
- Run `./target/release/ddb version` to check installation
- Review `config/` directory for configuration-based setup example
