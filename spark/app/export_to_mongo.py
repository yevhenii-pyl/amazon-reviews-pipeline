# spark/app/export_to_mongo.py
import os

from pyspark.sql import SparkSession
from utils.aggregate import (
    get_product_stats,
    get_verified_reviews_per_customer,
    get_monthly_reviews_per_product,
)
from utils.save_to_mongo import save_df_to_mongo  

mongo_uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_INITDB_DATABASE")

def main():
    spark = SparkSession.builder \
        .appName("Export Aggregates to MongoDB") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .getOrCreate()

    # Load cleaned data
    df_clean = spark.read.csv("/data/amazon_reviews_clean/", header=True, inferSchema=True)

    # Run aggregations
    product_stats = get_product_stats(df_clean)
    verified_per_customer = get_verified_reviews_per_customer(df_clean)
    monthly_reviews = get_monthly_reviews_per_product(df_clean)

    # Export using helper
    save_df_to_mongo(product_stats, mongo_uri, db_name, "product_stats")
    save_df_to_mongo(verified_per_customer, mongo_uri, db_name, "verified_reviews")
    save_df_to_mongo(monthly_reviews, mongo_uri, db_name, "monthly_trends")

    print("All aggregates saved to MongoDB.")

    spark.stop()

if __name__ == "__main__":
    main()
