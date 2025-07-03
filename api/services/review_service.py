import json
from datetime import datetime, timedelta
from collections import defaultdict

from db.cassandra_connector import get_cassandra_session
from models.review_models import Review
from utils.redis_client import redis_client

CACHE_TTL = 300

class ReviewService:
    def __init__(self):
        self.session = get_cassandra_session()

    def get_reviews_by_product(self, product_id: str) -> list[Review]:
        cache_key = f"reviews_by_product:{product_id}"
        cached = redis_client.get(cache_key)
        if cached:
            print("Cache hit")
            reviews_data = json.loads(cached)
            return [Review(**r) for r in reviews_data]

        print("Cache miss, querying Cassandra")

        query = """
            SELECT review_id, product_id, star_rating, customer_id, review_headline, review_body
            FROM reviews.reviews_by_product
            WHERE product_id = %s
            LIMIT 50
        """
        rows = self.session.execute(query, (product_id,))
        reviews = []
        for row in rows:
            reviews.append(Review(
                review_id=row.review_id,
                product_id=row.product_id,
                star_rating=row.star_rating,
                customer_id=row.customer_id,
                review_headline=row.review_headline,
                review_body=row.review_body
            ))

        redis_client.set(cache_key, json.dumps([r.dict() for r in reviews]), ex=CACHE_TTL)
        return reviews

    def get_reviews_by_product_and_rating(self, product_id: str, star_rating: int) -> list[Review]:
        cache_key = f"get_reviews_by_product_and_rating:{product_id}:{star_rating}"
        cached = redis_client.get(cache_key)
        if cached:
            print("Cache hit")
            reviews_data = json.loads(cached)
            return [Review(**r) for r in reviews_data]

        print("Cache miss, querying Cassandra")

        query = """
            SELECT review_id, product_id, star_rating, customer_id, review_headline, review_body
            FROM reviews.reviews_by_product
            WHERE product_id = %s AND star_rating = %s
            LIMIT 50
        """
        
        rows = self.session.execute(query, (product_id, star_rating))
        reviews = []

        for row in rows:
            reviews.append(Review(
                review_id=row.review_id,
                product_id=row.product_id,
                star_rating=row.star_rating,
                customer_id=row.customer_id,
                review_headline=row.review_headline,
                review_body=row.review_body
            ))

        redis_client.set(cache_key, json.dumps([r.dict() for r in reviews]), ex=CACHE_TTL)
        return reviews

    def get_reviews_by_customer(self, customer_id: str) -> list[Review]:
        cache_key = f"get_reviews_by_customer:{customer_id}"
        cached = redis_client.get(cache_key)
        if cached:
            print("Cache hit")
            reviews_data = json.loads(cached)
            return [Review(**r) for r in reviews_data]

        print("Cache miss, querying Cassandra")

        query = """
            SELECT review_id, product_id, star_rating, customer_id, review_headline, review_body
            FROM reviews.reviews_by_customer
            WHERE customer_id = %s
            LIMIT 50
        """

        rows = self.session.execute(query, (customer_id,))
        reviews = [
            Review(
                review_id=row.review_id,
                product_id=row.product_id,
                star_rating=row.star_rating,
                customer_id=row.customer_id,
                review_headline=row.review_headline,
                review_body=row.review_body
            )
            for row in rows
        ]

        redis_client.set(cache_key, json.dumps([r.dict() for r in reviews]), ex=CACHE_TTL)
        return reviews

    def get_top_n_reviewed_products_period(self, start_date: str, end_date: str, n: int) -> list[dict]:
        product_counts = defaultdict(int)

        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        current_date = start

        while current_date <= end:
            day_key = f"product_review_counts_by_date:{current_date.isoformat()}"
            cached_day = redis_client.get(day_key)

            if cached_day:
                print(f"Cache hit for {current_date}")
                day_data = json.loads(cached_day)
            else:
                print(f"Cache miss for {current_date}, querying Cassandra")
                query = """
                    SELECT product_id, review_count
                    FROM reviews.product_review_counts_by_date
                    WHERE review_date = %s
                """
                rows = self.session.execute(query, (current_date,))
                day_data = [{"product_id": row.product_id, "review_count": row.review_count} for row in rows]

                redis_client.set(day_key, json.dumps(day_data), ex=CACHE_TTL)

            for record in day_data:
                product_counts[record["product_id"]] += record["review_count"]

            current_date += timedelta(days=1)

        sorted_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)
        top_n = [{"product_id": pid, "review_count": count} for pid, count in sorted_products[:n]]

        return top_n
