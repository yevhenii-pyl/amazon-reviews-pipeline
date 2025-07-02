from fastapi import APIRouter, HTTPException
from typing import List
from services.review_service import ReviewService
from models.review_models import Review

router = APIRouter()
service = ReviewService()

@router.get("/reviews/product/{product_id}", response_model=List[Review])
async def get_reviews_by_product(product_id: str):
    reviews = service.get_reviews_by_product(product_id)
    if not reviews:
        raise HTTPException(status_code=404, detail="No reviews found for this product")
    return reviews
