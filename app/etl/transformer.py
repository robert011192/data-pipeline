"""Data transformation for market data."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from app.core.logging import get_logger
from app.schemas.market_data import MarketDataCreate

logger = get_logger(__name__)


class DataTransformer:
    """Transform raw API data into structured format."""

    def transform_alpha_vantage_data(
        self, ticker: str, raw_data: dict[str, Any]
    ) -> list[MarketDataCreate]:
        """
        Transform Alpha Vantage API response into MarketDataCreate schemas.

        Args:
            ticker: Stock ticker symbol
            raw_data: Raw API response from Alpha Vantage

        Returns:
            List of MarketDataCreate objects
        """
        transformed_data: list[MarketDataCreate] = []

        try:
            time_series = raw_data.get("Time Series (Daily)", {})

            for date_str, values in time_series.items():
                try:
                    # Parse date
                    trading_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                    # Extract and convert values
                    market_data = MarketDataCreate(
                        ticker=ticker.upper(),
                        date_=trading_date,
                        open_price=Decimal(values["1. open"]),
                        high_price=Decimal(values["2. high"]),
                        low_price=Decimal(values["3. low"]),
                        close_price=Decimal(values["4. close"]),
                        volume=int(values["5. volume"]),
                    )

                    transformed_data.append(market_data)

                except (KeyError, ValueError) as e:
                    logger.warning(
                        "Failed to parse data point",
                        ticker=ticker,
                        date=date_str,
                        error=str(e),
                    )
                    continue

            logger.info(
                "Data transformation completed",
                ticker=ticker,
                records_transformed=len(transformed_data),
            )

        except Exception as e:
            logger.error(
                "Error transforming data",
                ticker=ticker,
                error=str(e),
            )

        return transformed_data

    def transform_batch_data(
        self, raw_batch: dict[str, dict[str, Any]]
    ) -> dict[str, list[MarketDataCreate]]:
        """
        Transform multiple tickers' data.

        Args:
            raw_batch: Dictionary mapping tickers to raw API responses

        Returns:
            Dictionary mapping tickers to lists of MarketDataCreate objects
        """
        transformed_batch: dict[str, list[MarketDataCreate]] = {}

        for ticker, raw_data in raw_batch.items():
            transformed_data = self.transform_alpha_vantage_data(ticker, raw_data)
            if transformed_data:
                transformed_batch[ticker] = transformed_data

        total_records = sum(len(data) for data in transformed_batch.values())
        logger.info(
            "Batch transformation completed",
            tickers=len(transformed_batch),
            total_records=total_records,
        )

        return transformed_batch

    def validate_data(self, data: MarketDataCreate) -> bool:
        """
        Validate market data for consistency.

        Args:
            data: Market data to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            # Validate price relationships
            if not (data.low_price <= data.open_price <= data.high_price):
                logger.warning(
                    "Invalid price relationship: open",
                    ticker=data.ticker,
                    date=data.date_,
                )
                return False

            if not (data.low_price <= data.close_price <= data.high_price):
                logger.warning(
                    "Invalid price relationship: close",
                    ticker=data.ticker,
                    date=data.date_,
                )
                return False

            # Validate date is not in future
            if data.date_ > date.today():
                logger.warning(
                    "Future date detected",
                    ticker=data.ticker,
                    date=data.date_,
                )
                return False

            return True

        except Exception as e:
            logger.error(
                "Error validating data",
                ticker=data.ticker,
                error=str(e),
            )
            return False
