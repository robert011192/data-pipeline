"""Pytest configuration and fixtures with mocked database."""

from collections.abc import AsyncGenerator
from datetime import UTC, date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.db.models import MarketData
from app.schemas.market_data import MarketDataCreate


# Configure pytest-asyncio
@pytest.fixture(scope="session")
def anyio_backend():
    """Use asyncio backend for tests."""
    return "asyncio"


@pytest.fixture
def mock_db_session():
    """Create a mocked database session."""
    mock_session = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session.close = AsyncMock()
    mock_session.refresh = AsyncMock()
    mock_session.execute = AsyncMock()
    mock_session.add = MagicMock()
    return mock_session


@pytest.fixture
def sample_market_data() -> MarketDataCreate:
    """Create sample market data for testing."""
    return MarketDataCreate(
        ticker="AAPL",
        date=date(2024, 1, 15),
        open=Decimal("150.00"),
        high=Decimal("155.00"),
        low=Decimal("149.00"),
        close=Decimal("153.00"),
        volume=1000000,
    )


@pytest.fixture
def sample_market_data_model() -> MarketData:
    """Create sample market data model instance."""
    from datetime import datetime

    data = MarketData(
        ticker="AAPL",
        date=date(2024, 1, 15),
        open=Decimal("150.00"),
        high=Decimal("155.00"),
        low=Decimal("149.00"),
        close=Decimal("153.00"),
        volume=1000000,
    )
    data.id = 1  # Set ID for testing
    data.created_at = datetime.now(UTC)
    data.updated_at = datetime.now(UTC)
    return data


@pytest.fixture
def sample_alpha_vantage_response() -> dict:
    """Sample Alpha Vantage API response."""
    return {
        "Meta Data": {
            "1. Information": "Daily Prices",
            "2. Symbol": "AAPL",
        },
        "Time Series (Daily)": {
            "2024-01-15": {
                "1. open": "150.0000",
                "2. high": "155.0000",
                "3. low": "149.0000",
                "4. close": "153.0000",
                "5. volume": "1000000",
            },
            "2024-01-14": {
                "1. open": "148.0000",
                "2. high": "152.0000",
                "3. low": "147.0000",
                "4. close": "151.0000",
                "5. volume": "950000",
            },
        },
    }


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Create test client with mocked database dependency."""
    from app.api.deps import get_db
    from app.main import app

    # Mock the database dependency
    async def override_get_db():
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.close = AsyncMock()
        try:
            yield mock_session
        finally:
            await mock_session.close()

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac

    # Clean up
    app.dependency_overrides.clear()


@pytest.fixture
def mock_market_data_service():
    """Create a mocked MarketDataService."""
    mock_service = AsyncMock()
    mock_service.create = AsyncMock()
    mock_service.get_by_id = AsyncMock()
    mock_service.get_by_ticker_and_date = AsyncMock()
    mock_service.update = AsyncMock()
    mock_service.delete = AsyncMock()
    mock_service.get_tickers = AsyncMock()
    mock_service.build_query = MagicMock()
    mock_service.get_latest_date_for_ticker = AsyncMock()
    return mock_service


@pytest.fixture
def mock_extractor():
    """Create a mocked DataExtractor."""
    mock_ext = AsyncMock()
    mock_ext.fetch_daily_data = AsyncMock()
    mock_ext.fetch_batch_data = AsyncMock()
    return mock_ext


@pytest.fixture
def mock_loader():
    """Create a mocked DataLoader."""
    mock_load = AsyncMock()
    mock_load.load_market_data = AsyncMock()
    mock_load.load_batch = AsyncMock()
    return mock_load
