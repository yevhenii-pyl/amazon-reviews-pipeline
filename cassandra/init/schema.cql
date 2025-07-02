-- 1. Keyspace
CREATE KEYSPACE IF NOT EXISTS reviews
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

USE reviews;

-- 2. Reviews by product_id + star_rating
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id text,
    star_rating int,
    review_id text,
    customer_id text,
    review_headline text,
    review_body text,
    PRIMARY KEY ((product_id), star_rating, review_id)
);

-- 3. Reviews by customer_id
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id text,
    review_id text,
    product_id text,
    star_rating int,
    review_headline text,
    review_body text,
    PRIMARY KEY ((customer_id), review_id)
);

-- 4. Product review counts by date
CREATE TABLE IF NOT EXISTS product_review_counts_by_date (
    review_date date,
    product_id text,
    review_count int,
    PRIMARY KEY ((review_date), product_id)
);

-- 5. Verified reviews per customer by date
CREATE TABLE IF NOT EXISTS verified_review_counts_by_date (
    review_date date,
    customer_id text,
    review_count int,
    PRIMARY KEY ((review_date), customer_id)
);

-- 6. Haters (1–2 star) reviews per customer by date
CREATE TABLE IF NOT EXISTS haters_review_counts_by_date (
    review_date date,
    customer_id text,
    review_count int,
    PRIMARY KEY ((review_date), customer_id)
);

-- 7. Backers (4–5 star) reviews per customer by date
CREATE TABLE IF NOT EXISTS backers_review_counts_by_date (
    review_date date,
    customer_id text,
    review_count int,
    PRIMARY KEY ((review_date), customer_id)
);
