-- ============================================================================
-- DDB Table Schema - Sales Data
-- ============================================================================
-- This file demonstrates both table setup and example queries for DDB.

-- ============================================================================
-- TABLE SETUP
-- ============================================================================

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
-- Create a SQL schema file (e.g., ~/.ddb/schemas/sales_data.sql):

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

-- Then query without --file flag:
--   ddb --config examples/config --query "SELECT * FROM sales_data"

-- You can also use SET statements to configure session variables:
-- SET output_format = 'json';
-- SET max_rows = 100;

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================
-- All queries below work with BOTH methods (direct file or config-based)

-- METHOD 1 Usage: Direct file query
--   ddb --query "QUERY_HERE" --file examples/sales_data.csv
--
-- METHOD 2 Usage: Config-based query
--   ddb --config examples/config --query "QUERY_HERE"

-- 1. Get all orders
SELECT * FROM sales_data;

-- 2. Get first 5 orders
SELECT * FROM sales_data LIMIT 5;

-- 3. Find high-value orders (price > $500)
SELECT order_id, customer_name, price FROM sales_data WHERE price > 500;

-- 4. Orders from specific region
SELECT * FROM sales_data WHERE region = 'West';

-- 5. Multiple conditions (high value AND shipped)
SELECT * FROM sales_data WHERE price > 100 AND status = 'shipped';

-- 6. Pattern matching (find all laptop orders)
SELECT * FROM sales_data WHERE product LIKE '%Laptop%';

-- 7. Sort by price (highest first)
SELECT order_id, product, price FROM sales_data ORDER BY price DESC;

-- 8. Get top 5 most expensive orders
SELECT order_id, product, price FROM sales_data ORDER BY price DESC LIMIT 5;

-- 9. Count orders by region (GROUP BY)
SELECT region, COUNT(*) as order_count FROM sales_data GROUP BY region;

-- 10. Count orders by region with filtering (HAVING)
SELECT region, COUNT(*) as order_count
FROM sales_data
GROUP BY region
HAVING COUNT(*) > 4;

-- 11. Calculate total revenue
SELECT SUM(price * quantity) as total_revenue FROM sales_data;

-- 12. Average order value by region
SELECT region, ROUND(AVG(price), 2) as avg_price
FROM sales_data
GROUP BY region
ORDER BY avg_price DESC;

-- 13. Using functions (uppercase customer names)
SELECT order_id, UPPER(customer_name) as name, product
FROM sales_data
LIMIT 5;

-- 14. Calculate order total (price * quantity)
SELECT order_id, product, quantity, price,
       ROUND(price * quantity, 2) as total
FROM sales_data;

-- 15. Extract date parts
SELECT order_id, order_date,
       YEAR(order_date) as year,
       MONTH(order_date) as month
FROM sales_data
LIMIT 5;

-- 16. Conditional logic (status indicator)
SELECT order_id, status,
       IF(status = 'shipped', 'Complete', 'In Progress') as indicator
FROM sales_data;

-- 17. Get unique products
SELECT DISTINCT product FROM sales_data ORDER BY product;

-- 18. Get unique regions
SELECT DISTINCT region FROM sales_data ORDER BY region;

-- 19. Complex query: Filter, sort, and limit
SELECT order_id, customer_name, product, price
FROM sales_data
WHERE status = 'shipped' AND price > 200
ORDER BY price DESC
LIMIT 10;

-- 20. Multiple functions combined
SELECT order_id,
       UPPER(SUBSTR(customer_name, 1, 1)) as initial,
       LOWER(product) as prod,
       ROUND(price, 0) as price
FROM sales_data
WHERE price > 100;
