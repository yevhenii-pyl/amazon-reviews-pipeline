from db.cassandra_connector import get_cassandra_session
from models.review_models import Review

class ReviewService:
    def __init__(self):
        self.session = get_cassandra_session()

    def get_reviews_by_product(self, product_id: str) -> list[Review]:
        print(f"Querying Cassandra with product_id: '{product_id}'")  # <-- Log here

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
        return reviews
