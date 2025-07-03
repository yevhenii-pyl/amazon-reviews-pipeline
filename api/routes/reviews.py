from fastapi import APIRouter, HTTPException, Query
from typing import List
from services.review_service import ReviewService
from models.review_models import Review

router = APIRouter()
service = ReviewService()

@router.get("/product/{product_id}", response_model=List[Review])
async def get_reviews_by_product(product_id: str):
    reviews = service.get_reviews_by_product(product_id)
    if not reviews:
        raise HTTPException(status_code=404, detail="No reviews found for this product")
    return reviews

@router.get("/product/{product_id}/rating/{star_rating}", response_model=List[Review])
async def get_reviews_by_product_and_rating(product_id: str, star_rating: int):
    reviews = service.get_reviews_by_product_and_rating(product_id, star_rating)
    if not reviews:
        raise HTTPException(status_code=404, detail="No reviews found for this product with the specified rating")
    return reviews

@router.get("/customer/{customer_id}", response_model=List[Review])
async def get_reviews_by_customer(customer_id: str):
    print(f"Received customer_id: '{customer_id}'")
    reviews = service.get_reviews_by_customer(customer_id)
    if not reviews:
        raise HTTPException(status_code=404, detail="No reviews found for this customer")
    return reviews


@router.get("/top-products", response_model=List[dict])
async def get_top_products(
    start_date: str, 
    end_date: str, 
    n: int = Query(10, gt=0, le=100)
):
    products = service.get_top_n_reviewed_products_period(start_date, end_date, n)
    if not products:
        raise HTTPException(status_code=404, detail="No data found")
    return products
