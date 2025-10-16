from fastapi import APIRouter

from app.api.v1.endpoints import market_data

api_router = APIRouter()

api_router.include_router(market_data.router, tags=["Market data"])
