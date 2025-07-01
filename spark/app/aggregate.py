# spark/app/aggregate.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, to_date, date_format

def main():
    spark = SparkSession.builder \
        .appName("Amazon Reviews Aggregation") \
        .getOrCreate()

    # Load cleaned data
    df = spark.read.csv("/data/amazon_reviews_clean/", header=True, inferSchema=True)

    print("\nSchema of Cleaned Data:")
    df.printSchema()

    ### 1. Total number of reviews and average star rating per product
    product_stats = df.groupBy("product_id").agg(
        count("*").alias("total_reviews"),
        avg("star_rating").alias("avg_rating")
    )

    print("\nProduct Stats (total reviews + average rating):")
    product_stats.show(10, truncate=False)

    ### 2. Total number of verified reviews submitted by each customer
    verified_reviews_per_customer = df.groupBy("customer_id").agg(
        count("*").alias("verified_reviews")
    )

    print("\nVerified Reviews per Customer:")
    verified_reviews_per_customer.show(10, truncate=False)

    ### 3. Monthly number of reviews per product
    df = df.withColumn("review_month", date_format(col("review_date"), "yyyy-MM"))
    monthly_reviews = df.groupBy("product_id", "review_month").agg(
        count("*").alias("monthly_review_count")
    )

    print("\nMonthly Reviews per Product:")
    monthly_reviews.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
