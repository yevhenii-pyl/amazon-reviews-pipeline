from typing import List
from datetime import datetime, timedelta
import json

from models.customer_models import CustomerActivity 
from db.cassandra_connector import get_cassandra_session
from utils.redis_client import redis_client

CACHE_TTL = 300

class CustomerService:
    def __init__(self):
        self.session = get_cassandra_session()

    def get_most_productive_customers(self, start_date: str, end_date: str, limit: int) -> List[CustomerActivity]:
        table = "verified_review_counts_by_date"
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        counts = {}
        current_day = start
        while current_day <= end:
            day_str = current_day.isoformat()
            cache_key = f"verified_counts:{day_str}"

            cached = redis_client.get(cache_key)
            if cached:
                print(f"Cache hit for {cache_key}")
                day_counts = json.loads(cached)
            else:
                print(f"Cache miss for {cache_key}, querying Cassandra")
                query = f"""
                    SELECT customer_id, review_count
                    FROM reviews.{table}
                    WHERE review_date = %s
                """
                rows = self.session.execute(query, (day_str,))
                day_counts = {}
                for row in rows:
                    day_counts[row.customer_id] = row.review_count

                redis_client.set(cache_key, json.dumps(day_counts), ex=CACHE_TTL)

            for cid, count in day_counts.items():
                counts[cid] = counts.get(cid, 0) + count

            current_day += timedelta(days=1)

        # Sort by total counts descending, limit to N
        top_customers = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [CustomerActivity(customer_id=cid, review_count=count) for cid, count in top_customers]

    def get_top_haters(self, start_date: str, end_date: str, limit: int) -> list[CustomerActivity]:

        table = "haters_review_counts_by_date"
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        counts = {}
        current_day = start
        while current_day <= end:
            day_str = current_day.isoformat()
            cache_key = f"haters_counts:{day_str}"

            cached = redis_client.get(cache_key)
            if cached:
                print(f"Cache hit for {cache_key}")
                day_counts = json.loads(cached)
            else:
                print(f"Cache miss for {cache_key}, querying Cassandra")
                query = f"""
                    SELECT customer_id, review_count
                    FROM reviews.{table}
                    WHERE review_date = %s
                """
                rows = self.session.execute(query, (day_str,))
                day_counts = {}
                for row in rows:
                    day_counts[row.customer_id] = row.review_count

                redis_client.set(cache_key, json.dumps(day_counts), ex=CACHE_TTL)

            for cid, count in day_counts.items():
                counts[cid] = counts.get(cid, 0) + count

            current_day += timedelta(days=1)

        top_haters = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
        return [CustomerActivity(customer_id=cid, review_count=count, activity_type = 'hater') for cid, count in top_haters]

    def get_top_backers(self, start_date: str, end_date: str, limit: int) -> List[CustomerActivity]:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            delta = timedelta(days=1)
            counts = {}

            current = start
            while current <= end:
                day_str = current.strftime("%Y-%m-%d")
                cache_key = f"top_backers:{day_str}"

                cached = redis_client.get(cache_key)
                if cached:
                    daily_counts = json.loads(cached)
                else:
                    query = """
                        SELECT customer_id, review_count 
                        FROM reviews.backers_review_counts_by_date
                        WHERE review_date = %s
                    """
                    rows = self.session.execute(query, (day_str,))
                    daily_counts = {row.customer_id: row.review_count for row in rows}
                    redis_client.set(cache_key, json.dumps(daily_counts), ex=CACHE_TTL)

                for cid, cnt in daily_counts.items():
                    counts[cid] = counts.get(cid, 0) + cnt

                current += delta

            # Sort top N
            top_backers = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]

            return [CustomerActivity(customer_id=cid, review_count=count, activity_type="backer") for cid, count in top_backers]