from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="gold.dim_franchise",
    comment="Gold layer: Franchise dimension table with franchise location attributes"
)
def dim_franchise():
    return (
        spark.read.table("silver.bakehouse_franchises_silver")
        .select(
            F.col("franchiseID").alias("franchise_id"),
            "name",
            "region",
            "city"
        )
    )
