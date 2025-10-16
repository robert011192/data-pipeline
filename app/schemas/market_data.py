"""Pydantic schemas for market data API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class MarketDataBase(BaseModel):
    """Base schema for market data."""

    ticker: str = Field(..., min_length=1, max_length=10, description="Stock ticker symbol")
    date_: date = Field(..., alias="date", description="Trading date")
    open_price: Decimal = Field(..., alias="open", gt=0, description="Opening price")
    high_price: Decimal = Field(..., alias="high", gt=0, description="Highest price")
    low_price: Decimal = Field(..., alias="low", gt=0, description="Lowest price")
    close_price: Decimal = Field(..., alias="close", gt=0, description="Closing price")
    volume: int = Field(..., gt=0, description="Trading volume")

    model_config = ConfigDict(populate_by_name=True)


class MarketDataCreate(MarketDataBase):
    """Schema for creating market data."""

    pass


class MarketDataUpdate(BaseModel):
    """Schema for updating market data."""

    open_price: Decimal | None = Field(None, alias="open", gt=0)
    high_price: Decimal | None = Field(None, alias="high", gt=0)
    low_price: Decimal | None = Field(None, alias="low", gt=0)
    close_price: Decimal | None = Field(None, alias="close", gt=0)
    volume: int | None = Field(None, gt=0)

    model_config = ConfigDict(populate_by_name=True)


class MarketDataInDB(MarketDataBase):
    """Schema for market data stored in database."""

    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class MarketDataResponse(MarketDataInDB):
    """Schema for market data API response."""

    pass


class HealthCheck(BaseModel):
    """Schema for health check response."""

    status: str
    timestamp: datetime
    version: str
    database: str
