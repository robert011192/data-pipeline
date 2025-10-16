"""Tests for data transformer."""

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.etl.transformer import DataTransformer
from app.schemas.market_data import MarketDataCreate


@pytest.fixture
def transformer() -> DataTransformer:
    """Create transformer instance."""
    return DataTransformer()


def test_transform_alpha_vantage_data(
    transformer: DataTransformer, sample_alpha_vantage_response: dict
):
    """Test transforming Alpha Vantage data."""
    result = transformer.transform_alpha_vantage_data("AAPL", sample_alpha_vantage_response)

    assert len(result) == 2
    assert all(isinstance(item, MarketDataCreate) for item in result)
    assert result[0].ticker == "AAPL"
    assert result[0].date_ == date(2024, 1, 15)
    assert result[0].open_price == Decimal("150.0000")


def test_transform_empty_data(transformer: DataTransformer):
    """Test transforming empty data."""
    empty_data = {"Time Series (Daily)": {}}
    result = transformer.transform_alpha_vantage_data("AAPL", empty_data)
    assert len(result) == 0


def test_transform_invalid_data(transformer: DataTransformer):
    """Test transforming invalid data."""
    invalid_data = {
        "Time Series (Daily)": {
            "2024-01-15": {
                "1. open": "invalid",
                "2. high": "155.00",
                "3. low": "149.00",
                "4. close": "153.00",
                "5. volume": "1000000",
            }
        }
    }
    result = transformer.transform_alpha_vantage_data("AAPL", invalid_data)
    assert len(result) == 0


def test_validate_data_valid(transformer: DataTransformer):
    """Test validating valid data."""
    data = MarketDataCreate(
        ticker="AAPL",
        date=date(2024, 1, 15),
        open=Decimal("150.00"),
        high=Decimal("155.00"),
        low=Decimal("149.00"),
        close=Decimal("153.00"),
        volume=1000000,
    )
    assert transformer.validate_data(data) is True


def test_validate_data_invalid_price_relationship(transformer: DataTransformer):
    """Test validating data with invalid price relationships."""
    # Open price higher than high
    data = MarketDataCreate(
        ticker="AAPL",
        date=date(2024, 1, 15),
        open=Decimal("160.00"),
        high=Decimal("155.00"),
        low=Decimal("149.00"),
        close=Decimal("153.00"),
        volume=1000000,
    )
    assert transformer.validate_data(data) is False


def test_validate_data_future_date(transformer: DataTransformer):
    """Test validating data with future date."""
    future_date = date.today() + timedelta(days=1)

    data = MarketDataCreate(
        ticker="AAPL",
        date=future_date,
        open=Decimal("150.00"),
        high=Decimal("155.00"),
        low=Decimal("149.00"),
        close=Decimal("153.00"),
        volume=1000000,
    )
    assert transformer.validate_data(data) is False


def test_transform_batch_data(transformer: DataTransformer, sample_alpha_vantage_response: dict):
    """Test transforming batch data."""
    batch = {
        "AAPL": sample_alpha_vantage_response,
        "GOOGL": sample_alpha_vantage_response,
    }

    result = transformer.transform_batch_data(batch)

    assert len(result) == 2
    assert "AAPL" in result
    assert "GOOGL" in result
    assert len(result["AAPL"]) == 2
