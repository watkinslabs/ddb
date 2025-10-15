-- DDB Table Schema Documentation
-- This documents the structure of the sales_data table

-- NOTE: CREATE TABLE is not yet implemented in DDB v2.
-- This is for documentation purposes only.

-- Table Structure (sales_data.csv):
--   order_id       INTEGER
--   customer_name  STRING
--   product        STRING
--   quantity       INTEGER
--   price          FLOAT
--   order_date     DATE
--   region         STRING
--   status         STRING

-- METHOD 1: Direct File Query (Simple, for single file queries)
-- Usage:
--   ddb --query "SELECT * FROM sales_data" --file examples/sales_data.csv
--
-- In this mode:
-- - No configuration needed
-- - Table name can be anything (DDB ignores it)
-- - CSV headers automatically become column names
-- - Type inference is automatic

-- METHOD 2: Configuration-Based (For multiple tables, JOINs, etc.)
-- Create ~/.ddb/schemas/sales_data.yaml:
--
-- name: sales_data
-- database: main
-- data_file: /path/to/examples/sales_data.csv
-- field_delimiter: ','
-- data_starts_on: 1  # Skip header row
-- columns:
--   - name: order_id
--     type: Integer
--     nullable: false
--   - name: customer_name
--     type: String
--     nullable: false
--   - name: product
--     type: String
--     nullable: false
--   - name: quantity
--     type: Integer
--     nullable: false
--   - name: price
--     type: Float
--     nullable: false
--   - name: order_date
--     type: Date
--     nullable: false
--   - name: region
--     type: String
--     nullable: false
--   - name: status
--     type: String
--     nullable: false
--
-- Then query without --file flag:
--   ddb --query "SELECT * FROM sales_data"

-- Example queries for this table:

-- 1. Get all orders
-- SELECT * FROM sales_data;

-- 2. Find high-value orders
-- SELECT order_id, customer_name, price FROM sales_data WHERE price > 500;

-- 3. Count orders by region
-- SELECT region, COUNT(*) as order_count FROM sales_data GROUP BY region;

-- 4. Get shipped orders sorted by date
-- SELECT * FROM sales_data WHERE status = 'shipped' ORDER BY order_date DESC;

-- 5. Calculate total revenue
-- SELECT SUM(price * quantity) as total_revenue FROM sales_data;

-- 6. Find orders with specific products
-- SELECT * FROM sales_data WHERE product LIKE '%Laptop%';
