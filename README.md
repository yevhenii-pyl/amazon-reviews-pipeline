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
    - Spark (idle)
    - MongoDB (persistent)

### 5. Run Spark job

```bash
docker compose exec spark spark-submit /app/main.py
```

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