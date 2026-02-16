from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="gold.fact_sales",
    comment="Gold layer: Sales fact table with foreign keys to dimension tables"
)
def fact_sales():
    orders = spark.read.table("silver.bakehouse_orders_silver")
    franchises = spark.read.table("silver.bakehouse_franchises_silver")
    
    return (
        orders
        .join(franchises, orders.franchiseID == franchises.franchiseID, "left")
        .withColumn("date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast("int"))
        .select(
            F.col("transactionID").alias("sale_key"),
            F.col("customerID").alias("customer_key"),
            orders["franchiseID"].alias("franchise_key"),
            F.col("supplierID").alias("supplier_key"),
            "date_key",
            F.col("totalPrice").alias("revenue"),
            "quantity",
            "product"
        )
    )
