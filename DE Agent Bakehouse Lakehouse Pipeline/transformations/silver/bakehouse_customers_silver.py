from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver.bakehouse_customers_silver",
    comment="Silver layer: Standardized customer data with trimmed names and customer segment"
)
@dp.expect("valid_customer_id", "customerID IS NOT NULL")
def bakehouse_customers_silver():
    return (
        spark.read.table("bronze.bakehouse_customers_bronze")
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn(
            "customer_segment",
            F.when(F.col("country") == "United States", "Domestic")
            .otherwise("International")
        )
        .select(
            "customerID", "first_name", "last_name", "email_address",
            "phone_number", "address", "city", "state", "country",
            "continent", "postal_zip_code", "gender", "customer_segment"
        )
    )
