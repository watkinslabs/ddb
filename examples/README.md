# DDB Examples

This directory contains sample data and usage examples for DDB-Rust.

## Files

- **`sales_data.csv`** - Sample sales data with 20 orders
- **`schema.sql`** - Table schema definition and example queries
- **`WALKTHROUGH.md`** - Comprehensive guide with examples

## Quick Start

### 1. Build DDB

```bash
cargo build --release
```

### 2. Run a Simple Query

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data" \
  --file examples/sales_data.csv
```

### 3. Try Filtering

```bash
./target/release/ddb \
  --query "SELECT customer_name, product, price FROM sales_data WHERE price > 500" \
  --file examples/sales_data.csv
```

### 4. Sort and Limit

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data ORDER BY price DESC LIMIT 5" \
  --file examples/sales_data.csv
```

### 5. Use Functions

```bash
./target/release/ddb \
  --query "SELECT order_id, UPPER(customer_name) as name, ROUND(price, 0) FROM sales_data" \
  --file examples/sales_data.csv
```

### 6. Output as JSON

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West'" \
  --file examples/sales_data.csv \
  --output json
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

## Data Overview

The `sales_data.csv` contains:
- 20 orders
- Columns: order_id, customer_name, product, quantity, price, order_date, region, status
- Products: Laptop, Mouse, Keyboard, Monitor, Desk, Chair
- Regions: North, South, East, West
- Statuses: shipped, pending, processing, cancelled

## More Examples

See **`WALKTHROUGH.md`** for comprehensive examples including:
- Complex queries
- All 101 SQL functions
- Output format examples (JSON, YAML, CSV, Table)
- Custom delimiters
- Debug mode
- Performance tips
- Common patterns

## Need Help?

- Check `WALKTHROUGH.md` for detailed examples
- See `FUNCTIONS.md` in the project root for function reference
- Run `./target/release/ddb version` to check installation
