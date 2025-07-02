import json

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
