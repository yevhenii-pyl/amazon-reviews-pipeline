# spark/app/export_to_cassandre.py

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, trim, regexp_extract

class CassandraSeeder:
    def __init__(self):
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE")
        self.cassandra_host = os.getenv("CASSANDRA_HOST")
        self.spark = SparkSession.builder \
            .appName("AmazonReviewsSeeder") \
            .config("spark.cassandra.connection.host", self.cassandra_host) \
            .getOrCreate()
        
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

        self.df = self._load_and_clean_data()

    def _load_and_clean_data(self):
        df = self.spark.read.option("header", "true").csv("/data/amazon_reviews.csv")

        # Drop rows with nulls in critical columns
        df = df.dropna(subset=[
            "review_id",
            "product_id",
            "star_rating",
            "review_date",
            "customer_id"
        ])

        # Trim review_date to remove leading/trailing spaces
        df = df.withColumn("review_date_trimmed", trim(col("review_date")))

        # Extract a date-like substring from review_date (handles messy data)
        df = df.withColumn(
            "review_date_clean",
            regexp_extract(
                col("review_date_trimmed"),
                r"^(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})",
                1
            )
        )

        # Parse date in two common formats
        df = df.withColumn("review_date_parsed", to_date(col("review_date_clean"), "yyyy-MM-dd"))
        df = df.withColumn("review_date_parsed_alt", to_date(col("review_date_clean"), "M/d/yy"))

        # Choose the first valid parsed date
        df = df.withColumn(
            "review_date_final",
            coalesce(col("review_date_parsed"), col("review_date_parsed_alt"))
        )

        # Filter out rows where date parsing failed
        df = df.filter(col("review_date_final").isNotNull())

        # Cast star_rating to int
        df = df.withColumn("star_rating", col("star_rating").cast("int"))

        print("Data cleaned")
        df.printSchema()
        print(f"Cleaned rows: {df.count()}")

        return df
    
    def seed_reviews_by_product(self):
        self.df.select("review_id", "product_id", "star_rating", "customer_id", "review_headline", "review_body") \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="reviews_by_product", keyspace="reviews") \
            .option("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST")) \
            .save()

    def seed_reviews_by_customer(self):
        self.df.select("review_id", "customer_id", "product_id", "review_headline", "review_body", "star_rating") \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="reviews_by_customer", keyspace=self.keyspace) \
            .save()
        
    def seed_product_review_counts_by_date(self):
        self.df.groupBy(col("review_date_final").alias("review_date"), "product_id") \
            .count() \
            .withColumnRenamed("count", "review_count") \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="product_review_counts_by_date", keyspace=self.keyspace) \
            .save()
        
    def seed_verified_review_counts_by_date(self):
        self.df.filter(col("verified_purchase") == 1) \
            .groupBy(
                col("review_date_final").alias("review_date"),
                "customer_id"
            ) \
            .count() \
            .withColumnRenamed("count", "review_count") \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="verified_review_counts_by_date", keyspace=self.keyspace) \
            .save()

    def seed_haters_review_counts_by_date(self):
        self.df.filter((self.df["star_rating"] == "1") | (self.df["star_rating"] == "2")) \
            .groupBy(col("review_date_final").alias("review_date"), "customer_id") \
            .count() \
            .withColumnRenamed("count", "review_count") \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="haters_review_counts_by_date", keyspace=self.keyspace) \
            .save()
        
    def seed_backers_review_counts_by_date(self):
        self.df.filter((self.df["star_rating"] == "4") | (self.df["star_rating"] == "5")) \
            .groupBy(col("review_date_final").alias("review_date"), "customer_id") \
            .count() \
            .withColumnRenamed("count", "review_count") \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="backers_review_counts_by_date", keyspace=self.keyspace) \
            .save()

    def seed_all(self):
        self.seed_reviews_by_product()
        self.seed_reviews_by_customer()
        self.seed_product_review_counts_by_date()
        self.seed_verified_review_counts_by_date()
        self.seed_haters_review_counts_by_date()
        self.seed_backers_review_counts_by_date()

if __name__ == "__main__":
    seeder = CassandraSeeder()
    seeder.seed_all()
