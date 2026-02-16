from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="gold.dim_customer",
    comment="Gold layer: Customer dimension table with customer attributes"
)
def dim_customer():
    return (
        spark.read.table("silver.bakehouse_customers_silver")
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .select(
            F.col("customerID").alias("customer_id"),
            "full_name",
            F.col("customer_segment").alias("segment"),
            "country"
        )
    )
