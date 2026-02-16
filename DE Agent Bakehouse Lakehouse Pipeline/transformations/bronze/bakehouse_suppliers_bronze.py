from pyspark import pipelines as dp

@dp.table(
    name="bronze.bakehouse_suppliers_bronze",
    comment="Bronze layer: Raw copy of supplier data from samples.bakehouse.sales_suppliers"
)
def bakehouse_suppliers_bronze():
    return spark.read.table("samples.bakehouse.sales_suppliers")
