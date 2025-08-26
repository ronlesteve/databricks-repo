CREATE OR REFRESH MATERIALIZED VIEW lakeflow_demo.silver.customers_cleaned (
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL),
  CONSTRAINT email_not_null EXPECT (email IS NOT NULL),
  CONSTRAINT valid_email_format EXPECT (email LIKE '%@%')
) AS
SELECT DISTINCT
  CAST(customer_id AS INT) AS customer_id,
  TRIM(first_name) AS first_name,
  TRIM(last_name) AS last_name,
  LOWER(TRIM(email)) AS email,
  TO_DATE(signup_date, 'yyyy-MM-dd') AS signup_date
FROM lakeflow_demo.bronze.customers_raw
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
;

CREATE OR REFRESH MATERIALIZED VIEW lakeflow_demo.silver.sales_cleaned (
  CONSTRAINT transaction_id_not_null EXPECT (transaction_id IS NOT NULL),
  CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL),
  CONSTRAINT positive_sales_amount EXPECT (sales_amount > 0),
  CONSTRAINT positive_quantity EXPECT (quantity > 0),
  CONSTRAINT valid_payment_method EXPECT (payment_method IN ('card', 'paypal', 'cash'))
) AS
SELECT DISTINCT
  CAST(transaction_id AS INT) AS transaction_id,
  CAST(customer_id AS INT) AS customer_id,
  TO_DATE(transaction_date, 'yyyy-MM-dd') AS transaction_date,
  product_id,
  product_name,
  quantity,
  sales_amount,
  payment_method
FROM lakeflow_demo.bronze.sales_raw
WHERE transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND sales_amount > 0
  AND quantity > 0
  AND payment_method IN ('card', 'paypal', 'cash')
;
