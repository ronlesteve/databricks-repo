from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver.bakehouse_suppliers_silver",
    comment="Silver layer: Standardized supplier data with trimmed names and renamed category"
)
@dp.expect("valid_supplier_id", "supplierID IS NOT NULL")
def bakehouse_suppliers_silver():
    return (
        spark.read.table("bronze.bakehouse_suppliers_bronze")
        .withColumn("name", F.trim(F.col("name")))
        .withColumnRenamed("ingredient", "category")
        .select(
            "supplierID",
            "name",
            "category",
            "continent",
            "city",
            "district",
            "size",
            "longitude",
            "latitude",
            "approved"
        )
    )
