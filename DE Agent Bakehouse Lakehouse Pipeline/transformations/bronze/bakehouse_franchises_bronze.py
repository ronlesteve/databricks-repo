from pyspark import pipelines as dp

@dp.table(
    name="bronze.bakehouse_franchises_bronze",
    comment="Bronze layer: Raw copy of franchise data from samples.bakehouse.sales_franchises"
)
def bakehouse_franchises_bronze():
    return spark.read.table("samples.bakehouse.sales_franchises")
