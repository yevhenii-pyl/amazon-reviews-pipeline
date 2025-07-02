from pydantic import BaseModel
from typing import Optional

class Review(BaseModel):
    review_id: str
    product_id: str
    star_rating: int
    customer_id: str
    review_headline: Optional[str] = None
    review_body: Optional[str] = None
