from pyspark import pipelines as dp

@dp.table(
    name="bronze.bakehouse_customers_bronze",
    comment="Bronze layer: Raw copy of customer data from samples.bakehouse.sales_customers"
)
def bakehouse_customers_bronze():
    return spark.read.table("samples.bakehouse.sales_customers")
