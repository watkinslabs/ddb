-- Sales Data Table Definition
-- This CREATE TABLE statement defines the structure and location of the sales_data table.

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
FILE '/home/nd/repos/Projects/ddb/ddb-rust/examples/sales_data.csv'
DELIMITER ','
DATA_STARTS_ON 1
COMMENT_CHAR '#';
