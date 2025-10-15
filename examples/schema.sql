-- DDB Table Schema
-- This defines the structure of the sales_data table

CREATE TABLE sales_data (
    order_id INTEGER,
    customer_name STRING,
    product STRING,
    quantity INTEGER,
    price FLOAT,
    order_date DATE,
    region STRING,
    status STRING
) FILE 'examples/sales_data.csv' DELIMITER ',';

-- Usage:
-- ddb --query "SELECT * FROM sales_data" --file examples/sales_data.csv

-- Notes:
-- - The FILE clause specifies the path to the CSV file
-- - DELIMITER specifies the field separator (default is comma)
-- - DDB automatically detects headers in the first row
-- - Type inference happens automatically from the data

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
