"""Tests for market data API endpoints."""

from datetime import UTC
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from datetime import datetime

from app.db.models import MarketData
from app.schemas.market_data import MarketDataCreate


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    """Test health check endpoint."""
    with patch("app.api.v1.endpoints.market_data.get_db") as mock_get_db:
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_get_db.return_value = mock_session

        response = await client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "timestamp" in data
        assert "version" in data


@pytest.mark.asyncio
async def test_create_market_data(client: AsyncClient, sample_market_data: MarketDataCreate):
    """Test creating market data."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.get_by_ticker_and_date = AsyncMock(return_value=None)

        # Create a response object
        created_data = MarketData(
            id=1,
            ticker=sample_market_data.ticker,
            date=sample_market_data.date_,
            open=sample_market_data.open_price,
            high=sample_market_data.high_price,
            low=sample_market_data.low_price,
            close=sample_market_data.close_price,
            volume=sample_market_data.volume,
        )
        created_data.created_at = datetime.now(UTC)
        created_data.updated_at = datetime.now(UTC)
        mock_service.create = AsyncMock(return_value=created_data)
        mock_service_class.return_value = mock_service

        response = await client.post(
            "/api/v1/market-data",
            json=sample_market_data.model_dump(mode="json"),
        )
        assert response.status_code == 201
        data = response.json()
        assert data["ticker"] == sample_market_data.ticker
        assert data["date"] == str(sample_market_data.date_)


@pytest.mark.asyncio
async def test_create_duplicate_market_data(
    client: AsyncClient, sample_market_data: MarketDataCreate
):
    """Test creating duplicate market data returns 409."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()

        # Simulate existing data
        existing_data = MarketData(
            id=1,
            ticker=sample_market_data.ticker,
            date=sample_market_data.date_,
            open=sample_market_data.open_price,
            high=sample_market_data.high_price,
            low=sample_market_data.low_price,
            close=sample_market_data.close_price,
            volume=sample_market_data.volume,
        )
        existing_data.created_at = datetime.now(UTC)
        existing_data.updated_at = datetime.now(UTC)
        mock_service.get_by_ticker_and_date = AsyncMock(return_value=existing_data)
        mock_service_class.return_value = mock_service

        response = await client.post(
            "/api/v1/market-data",
            json=sample_market_data.model_dump(mode="json"),
        )
        assert response.status_code == 409


@pytest.mark.asyncio
async def test_list_market_data(client: AsyncClient, sample_market_data_model: MarketData):
    """Test listing market data."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = MagicMock()
        mock_service.build_query = MagicMock(return_value=select(MarketData))
        mock_service_class.return_value = mock_service

        with patch("app.api.v1.endpoints.market_data.paginate") as mock_paginate:
            # paginate is an async function, so we need AsyncMock
            mock_paginate_async = AsyncMock(return_value={
                "items": [sample_market_data_model],
                "total": 1,
                "page": 1,
                "size": 50,
                "pages": 1,
            })
            mock_paginate.side_effect = mock_paginate_async

            response = await client.get("/api/v1/market-data")
            assert response.status_code == 200
            data = response.json()
            assert "items" in data
            assert "total" in data
            assert "page" in data


@pytest.mark.asyncio
async def test_get_market_data_by_id(client: AsyncClient, sample_market_data_model: MarketData):
    """Test getting market data by ID."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.get_by_id = AsyncMock(return_value=sample_market_data_model)
        mock_service_class.return_value = mock_service

        response = await client.get(f"/api/v1/market-data/{sample_market_data_model.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == sample_market_data_model.id
        assert data["ticker"] == sample_market_data_model.ticker


@pytest.mark.asyncio
async def test_get_market_data_not_found(client: AsyncClient):
    """Test getting non-existent market data returns 404."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.get_by_id = AsyncMock(return_value=None)
        mock_service_class.return_value = mock_service

        response = await client.get("/api/v1/market-data/99999")
        assert response.status_code == 404


@pytest.mark.asyncio
async def test_update_market_data(client: AsyncClient, sample_market_data_model: MarketData):
    """Test updating market data."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()

        updated_data = MarketData(
            id=sample_market_data_model.id,
            ticker=sample_market_data_model.ticker,
            date=sample_market_data_model.date,
            open=sample_market_data_model.open,
            high=sample_market_data_model.high,
            low=sample_market_data_model.low,
            close=Decimal("160.00"),
            volume=2000000,
        )
        updated_data.created_at = datetime.now(UTC)
        updated_data.updated_at = datetime.now(UTC)
        mock_service.update = AsyncMock(return_value=updated_data)
        mock_service_class.return_value = mock_service

        update_payload = {
            "close": "160.00",
            "volume": 2000000,
        }

        response = await client.put(
            f"/api/v1/market-data/{sample_market_data_model.id}",
            json=update_payload,
        )
        assert response.status_code == 200
        data = response.json()
        assert float(data["close"]) == 160.00
        assert data["volume"] == 2000000


@pytest.mark.asyncio
async def test_update_market_data_not_found(client: AsyncClient):
    """Test updating non-existent market data returns 404."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.update = AsyncMock(return_value=None)
        mock_service_class.return_value = mock_service

        update_data = {"close": "160.00"}
        response = await client.put("/api/v1/market-data/99999", json=update_data)
        assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_market_data(client: AsyncClient, sample_market_data_model: MarketData):
    """Test deleting market data."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.delete = AsyncMock(return_value=True)
        mock_service_class.return_value = mock_service

        response = await client.delete(f"/api/v1/market-data/{sample_market_data_model.id}")
        assert response.status_code == 204


@pytest.mark.asyncio
async def test_delete_market_data_not_found(client: AsyncClient):
    """Test deleting non-existent market data returns 404."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.delete = AsyncMock(return_value=False)
        mock_service_class.return_value = mock_service

        response = await client.delete("/api/v1/market-data/99999")
        assert response.status_code == 404


@pytest.mark.asyncio
async def test_list_tickers(client: AsyncClient):
    """Test listing unique tickers."""
    with patch("app.api.v1.endpoints.market_data.MarketDataService") as mock_service_class:
        mock_service = AsyncMock()
        mock_service.get_tickers = AsyncMock(return_value=["AAPL", "GOOGL", "MSFT"])
        mock_service_class.return_value = mock_service

        response = await client.get("/api/v1/tickers")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "AAPL" in data
        assert len(data) == 3


@pytest.mark.asyncio
async def test_trigger_etl(client: AsyncClient):
    """Test manually triggering ETL pipeline."""
    with patch("app.api.v1.endpoints.market_data.ETLPipeline") as mock_pipeline_class:
        mock_pipeline = AsyncMock()
        mock_pipeline.run_batch = AsyncMock(return_value={
            "total_tickers": 3,
            "successful": 3,
            "failed": 0,
            "skipped": 0,
            "total_loaded": 300,
            "details": []
        })
        mock_pipeline_class.return_value = mock_pipeline

        response = await client.post("/api/v1/etl/run")
        assert response.status_code == 200
        data = response.json()
        assert "total_tickers" in data
        assert "successful" in data
        assert data["successful"] == 3
