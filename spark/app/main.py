# spark/app/main.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col

class AmazonReviewsPipeline:
    def __init__(self, spark, data_path, output_path):
        self.spark = spark
        self.data_path = data_path
        self.output_path = output_path
        self.df_raw = None
        self.df_clean = None

    def load_data(self):
        self.df_raw = self.spark.read.csv(self.data_path, header=True, inferSchema=True)
        print("Loaded CSV")
        self.df_raw.printSchema()
        print(f"Total rows: {self.df_raw.count()}")

    def clean_data(self):
        # drop nulls in key columns
        self.df_clean = self.df_raw.dropna(subset=["review_id", "product_id", "star_rating", "review_date"])

        # convert review_date to DateType
        self.df_clean = self.df_clean.withColumn("review_date", to_date("review_date", "yyyy-MM-dd"))

        # filter verified purchases
        self.df_clean = self.df_clean.filter(self.df_clean["verified_purchase"] == 1)

        print("Data cleaned")
        self.df_clean.printSchema()
        print(f"Cleaned rows: {self.df_clean.count()}")

    def cast_fields(self):
        self.df_clean = self.df_clean \
            .withColumn("star_rating", col("star_rating").cast("int")) \
            .withColumn("helpful_votes", col("helpful_votes").cast("int")) \
            .withColumn("total_votes", col("total_votes").cast("int")) \
            .withColumn("verified_purchase", col("verified_purchase").cast("int")) 

        print("Fields casted to numeric types.")
        self.df_clean.printSchema()

    def save_clean_data(self):
        self.df_clean.coalesce(1).write.csv(self.output_path, header=True, mode="overwrite")
        print(f"Saved cleaned data to {self.output_path}")

def main():
    spark = SparkSession.builder \
        .appName("Amazon Reviews ETL") \
        .getOrCreate()

    pipeline = AmazonReviewsPipeline(
        spark,
        data_path="/data/amazon_reviews.csv",
        output_path="/data/amazon_reviews_clean" 
    )

    pipeline.load_data()
    pipeline.clean_data()
    pipeline.cast_fields()
    pipeline.save_clean_data()

    spark.stop()


if __name__ == "__main__":
    main()
