from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="gold.dim_date",
    comment="Gold layer: Date dimension table with date attributes"
)
def dim_date():
    return (
        spark.read.table("silver.bakehouse_orders_silver")
        .select("order_date")
        .distinct()
        .withColumn("date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast("int"))
        .withColumn("date", F.col("order_date"))
        .withColumn("month", F.month(F.col("order_date")))
        .withColumn("quarter", F.quarter(F.col("order_date")))
        .withColumn("year", F.year(F.col("order_date")))
        .select(
            "date_key",
            "date",
            "month",
            "quarter",
            "year"
        )
    )
