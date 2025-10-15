-- Benchmark Data Table Definition
-- 2 million rows for performance testing

CREATE TABLE benchmark_data (
    order_id INTEGER,
    customer_name STRING,
    product STRING,
    quantity INTEGER,
    price FLOAT,
    order_date DATE,
    region STRING,
    status STRING
) FILE 'examples/benchmark_data.csv' DELIMITER ',';

-- This table contains 2,000,000 rows of sales order data
-- File size: ~123 MB
-- Used for performance benchmarking
