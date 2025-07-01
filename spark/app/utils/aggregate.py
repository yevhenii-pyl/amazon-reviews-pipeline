# spark/app/aggregate.py

from pyspark.sql.functions import col, avg, count, date_format
from pyspark.sql import SparkSession

def get_product_stats(df):
    return df.groupBy("product_id").agg(
        count("*").alias("total_reviews"),
        avg("star_rating").alias("avg_rating")
    )

def get_verified_reviews_per_customer(df):
    return df.groupBy("customer_id").agg(
        count("*").alias("verified_reviews")
    )

def get_monthly_reviews_per_product(df):
    df = df.withColumn("review_month", date_format(col("review_date"), "yyyy-MM"))
    return df.groupBy("product_id", "review_month").agg(
        count("*").alias("monthly_review_count")
    )

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("Amazon Reviews Aggregation") \
        .getOrCreate()

    df = spark.read.csv("/data/amazon_reviews_clean/", header=True, inferSchema=True)

    print("Product Stats:")
    get_product_stats(df).show(10, truncate=False)

    print("Verified Reviews per Customer:")
    get_verified_reviews_per_customer(df).show(10, truncate=False)

    print("Monthly Reviews per Product:")
