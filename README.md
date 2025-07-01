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

--- 

## Development Notes

1. Place your raw dataset (amazon_reviews.csv) into spark/data/

2. Query Mongo manually via:

```bash 
docker compose exec mongo mongosh
```

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