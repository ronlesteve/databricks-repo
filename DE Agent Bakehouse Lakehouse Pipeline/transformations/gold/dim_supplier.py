from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="gold.dim_supplier",
    comment="Gold layer: Supplier dimension table with supplier attributes"
)
def dim_supplier():
    return (
        spark.read.table("silver.bakehouse_suppliers_silver")
        .select(
            F.col("supplierID").alias("supplier_id"),
            "name",
            "category",
            "continent"
        )
    )
