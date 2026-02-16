from pyspark import pipelines as dp

@dp.table(
    name="bronze.bakehouse_orders_bronze",
    comment="Bronze layer: Raw copy of transaction data from samples.bakehouse.sales_transactions"
)
def bakehouse_orders_bronze():
    return spark.read.table("samples.bakehouse.sales_transactions")
