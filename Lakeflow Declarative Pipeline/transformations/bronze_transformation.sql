-- Create bronze streaming table for raw customer data selecting from a volume directory
CREATE OR REFRESH STREAMING TABLE lakeflow_demo.bronze.customers_raw
COMMENT "Raw customer data streaming from volume using cloud_files autoloader"
AS
SELECT * FROM cloud_files(
  'dbfs:/Volumes/lakeflow_demo/bronze/landing/customers',
  'csv',
  map('header', 'true', 'inferSchema', 'true')
);

-- Create bronze streaming table for raw sales data selecting from a volume directory
CREATE OR REFRESH STREAMING TABLE lakeflow_demo.bronze.sales_raw
COMMENT "Raw sales data streaming from volume using cloud_files autoloader"
AS
SELECT * FROM cloud_files(
  'dbfs:/Volumes/lakeflow_demo/bronze/landing/sales',
  'csv',
  map('header', 'true', 'inferSchema', 'true')
);