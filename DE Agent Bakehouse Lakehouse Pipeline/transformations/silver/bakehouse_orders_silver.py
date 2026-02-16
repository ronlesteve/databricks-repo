from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver.bakehouse_orders_silver",
    comment="Silver layer: Standardized order data with decimal pricing and date extraction"
)
@dp.expect("valid_total_price", "totalPrice >= 0")
@dp.expect("valid_customer_id", "customerID IS NOT NULL")
def bakehouse_orders_silver():
    return (
        spark.read.table("bronze.bakehouse_orders_bronze")
        .withColumn("totalPrice", F.col("totalPrice").cast("decimal(15,2)"))
        .withColumn("unitPrice", F.col("unitPrice").cast("decimal(15,2)"))
        .withColumn("order_date", F.to_date(F.col("dateTime")))
        .select(
            "transactionID",
            "customerID",
            "franchiseID",
            "dateTime",
            "order_date",
            "product",
            "quantity",
            "unitPrice",
            "totalPrice",
            "paymentMethod",
            "cardNumber"
        )
    )
