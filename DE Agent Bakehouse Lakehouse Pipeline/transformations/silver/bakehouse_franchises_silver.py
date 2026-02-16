from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver.bakehouse_franchises_silver",
    comment="Silver layer: Standardized franchise data with trimmed names and region mapping"
)
@dp.expect("valid_franchise_id", "franchiseID IS NOT NULL")
def bakehouse_franchises_silver():
    return (
        spark.read.table("bronze.bakehouse_franchises_bronze")
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("region", F.col("country"))
        .select(
            "franchiseID",
            "name",
            "city",
            "district",
            "zipcode",
            "country",
            "region",
            "size",
            "longitude",
            "latitude",
            "supplierID"
        )
    )
