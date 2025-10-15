-- Sales Data Table Definition
-- This table contains sales order information

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

-- Description:
-- This table tracks sales orders with customer information,
-- product details, pricing, and order status.
--
-- Columns:
-- - order_id: Unique order identifier
-- - customer_name: Name of the customer
-- - product: Product name
-- - quantity: Number of units ordered
-- - price: Unit price
-- - order_date: Date the order was placed
-- - region: Sales region (North, South, East, West)
-- - status: Order status (shipped, pending, processing, cancelled)
