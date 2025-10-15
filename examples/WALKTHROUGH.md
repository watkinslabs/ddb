# DDB-Rust Quick Walkthrough

This guide demonstrates how to use DDB-Rust to query CSV files with SQL.

## Sample Data

The `sales_data.csv` file contains 20 sales orders with the following columns:
- `order_id`: Order number
- `customer_name`: Customer name
- `product`: Product name
- `quantity`: Quantity ordered
- `price`: Unit price
- `order_date`: Order date
- `region`: Sales region (North, South, East, West)
- `status`: Order status (shipped, pending, processing, cancelled)

## Building DDB

```bash
cargo build --release
```

The binary will be at `./target/release/ddb`

## Basic Queries

### 1. Select All Data

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data" \
  --file examples/sales_data.csv
```

### 2. Select Specific Columns

```bash
./target/release/ddb \
  --query "SELECT order_id, customer_name, product, price FROM sales_data" \
  --file examples/sales_data.csv
```

### 3. Filter with WHERE Clause

**High-value orders (price > $500):**
```bash
./target/release/ddb \
  --query "SELECT order_id, customer_name, product, price FROM sales_data WHERE price > 500" \
  --file examples/sales_data.csv
```

**Orders from a specific region:**
```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West'" \
  --file examples/sales_data.csv
```

**Multiple conditions:**
```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE price > 100 AND status = 'shipped'" \
  --file examples/sales_data.csv
```

### 4. Pattern Matching with LIKE

**Find all laptop orders:**
```bash
./target/release/ddb \
  --query "SELECT order_id, customer_name, product FROM sales_data WHERE product LIKE '%Laptop%'" \
  --file examples/sales_data.csv
```

**Customers whose name starts with 'J':**
```bash
./target/release/ddb \
  --query "SELECT customer_name, product FROM sales_data WHERE customer_name LIKE 'J%'" \
  --file examples/sales_data.csv
```

### 5. Sorting with ORDER BY

**Sort by price (highest first):**
```bash
./target/release/ddb \
  --query "SELECT product, price FROM sales_data ORDER BY price DESC" \
  --file examples/sales_data.csv
```

**Sort by date and customer name:**
```bash
./target/release/ddb \
  --query "SELECT order_date, customer_name, product FROM sales_data ORDER BY order_date DESC, customer_name ASC" \
  --file examples/sales_data.csv
```

### 6. Limit Results

**Top 5 most expensive orders:**
```bash
./target/release/ddb \
  --query "SELECT order_id, product, price FROM sales_data ORDER BY price DESC LIMIT 5" \
  --file examples/sales_data.csv
```

**Skip first 5 results, get next 5:**
```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data LIMIT 5 OFFSET 5" \
  --file examples/sales_data.csv
```

### 7. DISTINCT Values

**Get unique products:**
```bash
./target/release/ddb \
  --query "SELECT DISTINCT product FROM sales_data" \
  --file examples/sales_data.csv
```

**Get unique regions:**
```bash
./target/release/ddb \
  --query "SELECT DISTINCT region FROM sales_data ORDER BY region" \
  --file examples/sales_data.csv
```

## Using Functions

### String Functions

**Uppercase customer names:**
```bash
./target/release/ddb \
  --query "SELECT order_id, UPPER(customer_name) as name, product FROM sales_data LIMIT 5" \
  --file examples/sales_data.csv
```

**Lowercase product names:**
```bash
./target/release/ddb \
  --query "SELECT LOWER(product) as product, price FROM sales_data" \
  --file examples/sales_data.csv
```

**Get first 10 characters of customer name:**
```bash
./target/release/ddb \
  --query "SELECT SUBSTR(customer_name, 1, 10) as name, product FROM sales_data" \
  --file examples/sales_data.csv
```

### Math Functions

**Round prices:**
```bash
./target/release/ddb \
  --query "SELECT product, ROUND(price, 0) as rounded_price FROM sales_data" \
  --file examples/sales_data.csv
```

**Calculate total per order (price * quantity):**
```bash
./target/release/ddb \
  --query "SELECT order_id, product, quantity, price, ROUND(price * quantity, 2) as total FROM sales_data" \
  --file examples/sales_data.csv
```

### Date Functions

**Extract year from order date:**
```bash
./target/release/ddb \
  --query "SELECT order_id, YEAR(order_date) as year, MONTH(order_date) as month FROM sales_data" \
  --file examples/sales_data.csv
```

**Format dates:**
```bash
./target/release/ddb \
  --query "SELECT order_id, DATE_FORMAT(order_date, '%Y-%m-%d') as formatted_date FROM sales_data LIMIT 5" \
  --file examples/sales_data.csv
```

### Conditional Functions

**Add status indicator:**
```bash
./target/release/ddb \
  --query "SELECT order_id, status, IF(status = 'shipped', 'Complete', 'In Progress') as indicator FROM sales_data" \
  --file examples/sales_data.csv
```

**Handle missing data:**
```bash
./target/release/ddb \
  --query "SELECT order_id, IFNULL(customer_name, 'Unknown') as name FROM sales_data" \
  --file examples/sales_data.csv
```

## Output Formats

### JSON Output

```bash
./target/release/ddb \
  --query "SELECT order_id, customer_name, product, price FROM sales_data WHERE price > 500" \
  --file examples/sales_data.csv \
  --output json
```

### YAML Output

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data LIMIT 3" \
  --file examples/sales_data.csv \
  --output yaml
```

### CSV Output

```bash
./target/release/ddb \
  --query "SELECT order_id, customer_name, price FROM sales_data WHERE region = 'West'" \
  --file examples/sales_data.csv \
  --output csv
```

### Pretty Table Output (Default)

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data LIMIT 5" \
  --file examples/sales_data.csv \
  --output table
```

## Custom Delimiters

### Tab-Separated Files

```bash
./target/release/ddb \
  --query "SELECT * FROM data" \
  --file your_file.tsv \
  --delimiter $'\t'
```

### Pipe-Separated Files

```bash
./target/release/ddb \
  --query "SELECT * FROM data" \
  --file your_file.txt \
  --delimiter '|'
```

## Debug Mode

See the tokenization and parsing process:

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE price > 100" \
  --file examples/sales_data.csv \
  --debug
```

This will show:
1. Token stream from lexer
2. Abstract Syntax Tree (AST) from parser
3. Number of results returned

## Complex Queries

### Filtered, Sorted, Limited Results

```bash
./target/release/ddb \
  --query "SELECT order_id, customer_name, product, price FROM sales_data WHERE status = 'shipped' AND price > 200 ORDER BY price DESC LIMIT 10" \
  --file examples/sales_data.csv
```

### Using Multiple Functions

```bash
./target/release/ddb \
  --query "SELECT order_id, UPPER(SUBSTR(customer_name, 1, 1)) as initial, LOWER(product) as prod, ROUND(price, 0) as price FROM sales_data WHERE price > 100" \
  --file examples/sales_data.csv
```

### Date Range Queries

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE order_date >= '2024-01-20' AND order_date <= '2024-01-25'" \
  --file examples/sales_data.csv
```

## Piping Output

### Save results to a file

```bash
./target/release/ddb \
  --query "SELECT * FROM sales_data WHERE region = 'West'" \
  --file examples/sales_data.csv \
  --output csv > west_region_sales.csv
```

### Use with other Unix tools

```bash
./target/release/ddb \
  --query "SELECT customer_name, price FROM sales_data WHERE price > 500" \
  --file examples/sales_data.csv \
  --output csv | sort -t, -k2 -n
```

## Available Functions

DDB supports 101 SQL functions across multiple categories:

- **Math**: ABS, CEIL, FLOOR, ROUND, SQRT, POW, MOD, SIGN, etc.
- **String**: UPPER, LOWER, TRIM, CONCAT, SUBSTR, REPLACE, LENGTH, etc.
- **Date/Time**: NOW, YEAR, MONTH, DAY, DATEDIFF, DATEADD, AGE, etc.
- **Conversion**: CAST, ATOF, ATOI, HEX, BIN, FORMAT, etc.
- **Conditional**: IF, IFNULL, COALESCE, NULLIF, GREATEST, LEAST, etc.
- **Utility**: HASH, BASE64_ENCODE, REGEXP_MATCH, SPLIT_PART, etc.

See `FUNCTIONS.md` for complete documentation.

## Performance Tips

1. **Use column selection**: Select only the columns you need instead of `SELECT *`
2. **Filter early**: Apply WHERE clauses to reduce the dataset size
3. **Use LIMIT**: If you only need a subset of results
4. **Streaming**: DDB processes files in a streaming manner, so memory usage stays low even with large files

## Common Patterns

### Data Exploration

```bash
# See first 10 rows
./target/release/ddb --query "SELECT * FROM data LIMIT 10" --file mydata.csv

# Count total rows
./target/release/ddb --query "SELECT COUNT(*) FROM data" --file mydata.csv

# Get unique values in a column
./target/release/ddb --query "SELECT DISTINCT column_name FROM data" --file mydata.csv
```

### Data Validation

```bash
# Find NULL or empty values
./target/release/ddb --query "SELECT * FROM data WHERE column IS NULL" --file mydata.csv

# Find duplicates
./target/release/ddb --query "SELECT column, COUNT(*) FROM data GROUP BY column HAVING COUNT(*) > 1" --file mydata.csv
```

### Data Transformation

```bash
# Clean and format data
./target/release/ddb --query "SELECT TRIM(name), UPPER(status), ROUND(amount, 2) FROM data" --file mydata.csv --output csv > cleaned.csv
```

## Next Steps

- Explore the full function reference in `FUNCTIONS.md`
- Check out the source code to understand the implementation
- Try DDB on your own CSV files
- Contribute improvements on GitHub!

## Troubleshooting

**Error: "Column not found"**
- Check that the column name matches exactly (case-sensitive)
- Verify the CSV has a header row

**Error: "Invalid delimiter"**
- Make sure the delimiter matches your file format
- For tabs, use `--delimiter $'\t'`

**No results returned**
- Check your WHERE clause conditions
- Try without WHERE to see all data first

**Performance issues with large files**
- DDB streams data, so it should handle large files well
- Consider using WHERE clauses to filter early
- Use LIMIT to test queries on subsets first
