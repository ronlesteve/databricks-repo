from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="silver.bakehouse_reviews_silver",
    comment="Silver layer: Standardized review data with sentiment score derived from review length"
)
@dp.expect("valid_review_date", "review_date IS NOT NULL")
def bakehouse_reviews_silver():
    return (
        spark.read.table("bronze.bakehouse_reviews_bronze")
        .withColumn(
            "sentiment_score",
            F.when(F.length(F.col("review")) < 50, 2.0)
            .when(F.length(F.col("review")) < 100, 3.0)
            .when(F.length(F.col("review")) < 200, 4.0)
            .otherwise(5.0)
            .cast("decimal(3,1)")
        )
        .select("new_id", "franchiseID", "review", "review_date", "sentiment_score")
    )
