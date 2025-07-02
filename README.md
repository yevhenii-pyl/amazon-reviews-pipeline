# Amazon Reviews Data Pipeline (Spark + MongoDB)

A fully Dockerized data pipeline to process large-scale Amazon reviews using Apache Spark and store aggregated results in MongoDB.

>  No local Python or Spark setup required — just Docker.

---

## Setup Instructions

### 1. Clone the repo

### 2. Create your .env file

### 3. Build the containers

```bash
docker compose build
```

### 4. Start services

```bash
docker compose up -d
```

This will start:
- **Spark** (idle, ready to submit jobs)  
- **MongoDB** (persistent local volume)

### 5. Run Spark job

To run the main Spark ETL pipeline (loads, cleans, casts, and saves the cleaned dataset):
```bash
docker compose exec spark spark-submit /app/main.py
```

This will:
- Load the raw CSV file from /data/amazon_reviews.csv
- Clean the dataset (drop nulls, filter verified purchases)
- Cast relevant fields to numeric types
- Save the cleaned result into a single CSV file inside the /data/amazon_reviews_clean/ folder

### 6. Debug / Explore Cleaned Data

You can run the debug script to inspect the cleaned output without re-running the whole pipeline:
```bash
docker compose exec spark spark-submit /app/debug_view.py
```

This script will:
- Load the cleaned data from the folder /data/amazon_reviews_clean/
- Infer schema
- Show sample rows
- Print total row count

**Important:**: Make sure the main ETL job (main.py) has run first — the debug script reads its output.

### 7. Export Aggregates to MongoDB

After cleaning and exploring the data, you can run the aggregation + export pipeline to compute insights and load them into MongoDB:

```bash
docker compose exec spark spark-submit /app/export_to_mongo.py
```


This script will:
- Load the cleaned data from /data/amazon_reviews_clean/
- Calculate three aggregate datasets:
    - Total reviews and average rating per product
    - Verified reviews per customer
    - Monthly review counts per product
- Save all aggregates into the following MongoDB collections:
    - product_stats
    - verified_reviews
    - monthly_trends

Ensure MongoDB is up and running and .env contains the correct MONGO_PORT and MONGO_INITDB_DATABASE values.

To verify, connect to the Mongo shell:
```bash
docker compose exec mongo mongosh
```

Querying example: 
```bash
// Average rating for a product
db.product_stats.find({ product_id: "0842329129" })

// Verified reviews by a customer
db.verified_reviews.find({ customer_id: 27851871 })

// Monthly review trend for a product
db.monthly_trends.find({ product_id: "0842329129" })
```

## 8. Initialize Cassandra Schema

Once all containers are up and Cassandra is running, execute:
```bash
docker compose exec cassandra cqlsh -f /schema-init/schema.cql
```

This command will:
- Create the required keyspace and tables inside Cassandra
- Prepare the schema needed for downstream PySpark and API integration

For confirmation, run:

```bash
docker compose exec cassandra cqlsh

USE reviews;
DESCRIBE TABLES;
```

Expected tables:
- reviews_by_product
- reviews_by_customer
- product_review_counts_by_date
- verified_review_counts_by_date
- haters_review_counts_by_date
- backers_review_counts_by_date

## 9. Seed Cassandra 

1. Make sure Cassandra and Spark containers are running.
2. Run the seeder script inside the Spark container:
```bash
docker compose exec spark spark-submit /app/export_to_cassandra.py
```

3. The script will clean the dataset and seed all tables automatically.

--- 

## Development Notes

1. Place your raw dataset (amazon_reviews.csv) into spark/data/

2. Query Mongo manually via:

```bash 
docker compose exec mongo mongosh
```

--- 

⚠️ Startup Sequence Caveats

The API depends on Cassandra being fully ready before starting, but currently there is no automatic wait or retry logic implemented.
This can cause initial API startup failures due to connection errors (cassandra.UnresolvableContactPoints).

When starting containers fresh, you may need to:

- Wait for Cassandra to initialize (including running migration scripts).
- Run migration & seeder scripts manually to populate the database (step 8 - 9).
- Restart or manually start the API container after Cassandra is ready.

Automating startup dependency management and migrations is planned for a future improvement.

---

## Cleanup

To stop everything and remove containers:

```bash
docker compose down
```

To also delete all volumes (MongoDB data):

```bash
docker compose down -v
```