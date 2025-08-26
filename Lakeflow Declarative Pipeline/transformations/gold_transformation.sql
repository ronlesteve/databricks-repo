-- Dimension Table: Customers
CREATE OR REFRESH MATERIALIZED VIEW lakeflow_demo.gold.dim_customers AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  signup_date
FROM lakeflow_demo.silver.customers_cleaned;

-- Dimension Table: Products
CREATE OR REFRESH MATERIALIZED VIEW lakeflow_demo.gold.dim_products AS 
SELECT DISTINCT
  product_id,
  product_name
FROM lakeflow_demo.silver.sales_cleaned;

-- Dimension Table: Dates
CREATE OR REFRESH MATERIALIZED VIEW lakeflow_demo.gold.dim_dates AS
SELECT DISTINCT
  transaction_date AS date,
  YEAR(transaction_date) AS year,
  MONTH(transaction_date) AS month,
  DAY(transaction_date) AS day,
  WEEKOFYEAR(transaction_date) AS week
FROM lakeflow_demo.silver.sales_cleaned;

-- Fact Table: Sales
CREATE OR REFRESH MATERIALIZED VIEW lakeflow_demo.gold.fact_sales AS
SELECT
  transaction_id,
  customer_id,
  product_id,
  transaction_date,
  quantity,
  sales_amount,
  payment_method
FROM lakeflow_demo.silver.sales_cleaned;