from fastapi import APIRouter, Query
from typing import List
from services.customer_service import CustomerService
from models.customer_models import CustomerActivity

router = APIRouter()
service = CustomerService()

@router.get("/most_productive", response_model=List[CustomerActivity])
async def get_most_productive_customers(
    start_date: str,
    end_date: str,
    limit: int = Query(10, gt=0, le=100)
):
    return service.get_most_productive_customers(start_date, end_date, limit)

@router.get("/haters", response_model=List[CustomerActivity])
async def get_top_haters(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD"),
    limit: int = Query(10, description="Max number of customers to return")
):
    return service.get_top_haters(start_date, end_date, limit)

@router.get("/backers", response_model=List[CustomerActivity])
async def get_top_backers(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD"),
    limit: int = Query(10, description="Max number of customers to return")
):
    return service.get_top_backers(start_date, end_date, limit)