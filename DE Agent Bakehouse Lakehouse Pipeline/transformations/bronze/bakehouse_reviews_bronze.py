from pyspark import pipelines as dp

@dp.table(
    name="bronze.bakehouse_reviews_bronze",
    comment="Bronze layer: Raw copy of customer review data from samples.bakehouse.media_customer_reviews"
)
def bakehouse_reviews_bronze():
    return spark.read.table("samples.bakehouse.media_customer_reviews")
