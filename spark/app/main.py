# spark/app/main.py

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Amazon Reviews ETL") \
        .getOrCreate()

    print("Spark session started successfully.")
    print("No logic implemented yet — ready for development.")

    spark.stop()

if __name__ == "__main__":
    main()
