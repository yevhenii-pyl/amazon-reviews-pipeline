from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("AmazonReviews Debug View") \
        .getOrCreate()

    # Load cleaned CSV output directory
    df = spark.read.csv("/data/amazon_reviews_clean/", header=True, inferSchema=True)

    # Show schema and sample rows
    print("Schema:")
    df.printSchema()

    print("\nSample rows:")
    df.show(20, truncate=False)

    # Optional: Count rows
    print(f"\nTotal cleaned rows: {df.count()}")

    spark.stop()

if __name__ == "__main__":
    main()
