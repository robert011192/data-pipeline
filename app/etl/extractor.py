"""Data extraction from Alpha Vantage API."""

from typing import Any

import httpx

from app.core.config import settings
from app.core.consts import (
    API_ERROR_MESSAGE_KEY,
    API_FUNCTION_TIME_SERIES_DAILY,
    API_INFORMATION_KEY,
    API_NOTE_KEY,
    API_OUTPUT_SIZE_COMPACT,
    API_TIME_SERIES_DAILY_KEY,
    HTTP_TIMEOUT_SECONDS,
)
from app.core.logging import get_logger

logger = get_logger(__name__)


class DataExtractor:
    """Extract stock market data from Alpha Vantage API."""

    def __init__(self) -> None:
        """Initialize the data extractor."""
        self.api_key = settings.ALPHA_VANTAGE_API_KEY
        self.base_url = settings.ALPHA_VANTAGE_BASE_URL
        self.timeout = HTTP_TIMEOUT_SECONDS

    async def fetch_daily_data(self, ticker: str) -> dict[str, Any] | None:
        """
        Fetch daily stock data for a ticker from Alpha Vantage.

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')

        Returns:
            Dictionary containing the API response, or None if failed
        """
        params = {
            "function": API_FUNCTION_TIME_SERIES_DAILY,
            "symbol": ticker,
            "apikey": self.api_key,
            "outputsize": API_OUTPUT_SIZE_COMPACT,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info("Fetching data for ticker", ticker=ticker)
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()

                data = response.json()

                # Validate API response using match-case
                match tuple(data.keys()):
                    case keys if API_ERROR_MESSAGE_KEY in keys:
                        logger.error(
                            "API error",
                            ticker=ticker,
                            error=data[API_ERROR_MESSAGE_KEY],
                        )
                        return None

                    case keys if API_NOTE_KEY in keys:
                        logger.warning(
                            "API rate limit or notice",
                            ticker=ticker,
                            note=data[API_NOTE_KEY],
                        )
                        return None

                    case keys if API_INFORMATION_KEY in keys:
                        logger.warning(
                            "API information message",
                            ticker=ticker,
                            info=data[API_INFORMATION_KEY],
                        )
                        return None

                    case keys if API_TIME_SERIES_DAILY_KEY in keys:
                        logger.info(
                            "Successfully fetched data",
                            ticker=ticker,
                            data_points=len(data[API_TIME_SERIES_DAILY_KEY]),
                        )
                        return data  # type: ignore[no-any-return]

                    case _:
                        logger.error(
                            "Unexpected API response format",
                            ticker=ticker,
                            keys=list(data.keys()),
                        )
                        return None

        except httpx.HTTPError as e:
            logger.error(
                "HTTP error while fetching data",
                ticker=ticker,
                error=str(e),
            )
            return None
        except Exception as e:
            logger.error(
                "Unexpected error while fetching data",
                ticker=ticker,
                error=str(e),
            )
            return None

    async def fetch_batch_data(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        """
        Fetch data for multiple tickers.

        Args:
            tickers: List of ticker symbols

        Returns:
            Dictionary mapping tickers to their data
        """
        results: dict[str, dict[str, Any]] = {}

        for ticker in tickers:
            data = await self.fetch_daily_data(ticker)
            if data:
                results[ticker] = data

        logger.info(
            "Batch fetch completed",
            total_tickers=len(tickers),
            successful=len(results),
            failed=len(tickers) - len(results),
        )

        return results
