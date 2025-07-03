from pydantic import BaseModel
from typing import Optional

class CustomerActivity(BaseModel):
    customer_id: str
    review_count: int
    activity_type: Optional[str] = None  # e.g. "backer", "hater"
