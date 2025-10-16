"""ETL Pipeline orchestration."""

from datetime import date, timedelta
from typing import Any, TypedDict

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.etl.extractor import DataExtractor
from app.etl.loader import DataLoader
from app.etl.transformer import DataTransformer
from app.services.market_data_service import MarketDataService

logger = get_logger(__name__)


class BatchStats(TypedDict):
    """Type definition for batch ETL statistics."""

    total_tickers: int
    successful: int
    failed: int
    skipped: int
    total_loaded: int
    details: list[dict[str, Any]]


class ETLPipeline:
    """Orchestrate the ETL process."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the ETL pipeline.

        Args:
            session: Database session
        """
        self.session = session
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader(session)
        self.service = MarketDataService(session)

    async def _should_skip_ticker(self, ticker: str) -> tuple[bool, str]:
        """
        Check if we should skip ETL for a ticker based on last update time.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Tuple of (should_skip, reason)
        """
        latest_date = await self.service.get_latest_date_for_ticker(ticker)

        if latest_date is None:
            # No data exists, fetch everything
            return False, "initial_load"

        today = date.today()
        days_since_update = (today - latest_date).days

        # If data is from today or yesterday (market might not have closed yet), skip
        if days_since_update <= 1:
            return True, f"data_current_last_update_{latest_date}"

        # If weekend, check Friday's data
        if today.weekday() in [5, 6]:  # Saturday or Sunday
            # Data should be up to Friday
            last_trading_day = today - timedelta(days=today.weekday() - 4)
            if latest_date >= last_trading_day:
                return True, f"weekend_data_current_{latest_date}"

        return False, f"needs_update_last_update_{latest_date}"

    async def run_for_ticker(
        self, ticker: str, force: bool = False, incremental: bool = True
    ) -> dict[str, Any]:
        """
        Run ETL pipeline for a single ticker with incremental loading.

        Args:
            ticker: Stock ticker symbol
            force: Force ETL even if data is current
            incremental: Only load new data

        Returns:
            Dictionary with pipeline execution statistics
        """
        logger.info("Starting ETL for ticker", ticker=ticker, incremental=incremental)

        stats = {
            "ticker": ticker,
            "extracted": 0,
            "transformed": 0,
            "loaded": 0,
            "skipped": False,
            "reason": "",
            "status": "failed",
        }

        try:
            if incremental and not force:
                should_skip, reason = await self._should_skip_ticker(ticker)
                if should_skip:
                    logger.info("Skipping ticker - data is current", ticker=ticker, reason=reason)
                    stats["skipped"] = True
                    stats["reason"] = reason
                    stats["status"] = "skipped"
                    return stats

            # Extract
            raw_data = await self.extractor.fetch_daily_data(ticker)
            if not raw_data:
                logger.warning("No data extracted", ticker=ticker)
                return stats

            stats["extracted"] = len(raw_data.get("Time Series (Daily)", {}))

            # Transform
            transformed_data = self.transformer.transform_alpha_vantage_data(ticker, raw_data)
            if not transformed_data:
                logger.warning("No data transformed", ticker=ticker)
                return stats

            stats["transformed"] = len(transformed_data)

            # Validate and filter
            valid_data = [
                data for data in transformed_data if self.transformer.validate_data(data)
            ]

            if not valid_data:
                logger.warning("No valid data after validation", ticker=ticker)
                return stats

            # For incremental mode, filter out data we already have
            if incremental:
                latest_date = await self.service.get_latest_date_for_ticker(ticker)
                if latest_date:
                    # Only load data newer than what we have, or update today's data
                    valid_data = [
                        data for data in valid_data
                        if data.date_ >= latest_date
                    ]
                    logger.info(
                        "Incremental load - filtering old data",
                        ticker=ticker,
                        cutoff_date=latest_date,
                        records_to_load=len(valid_data)
                    )

            if not valid_data:
                logger.info("No new data to load", ticker=ticker)
                stats["status"] = "no_new_data"
                return stats

            # Load
            loaded_count = await self.loader.load_batch(valid_data)
            stats["loaded"] = loaded_count
            stats["status"] = "success" if loaded_count > 0 else "failed"

            logger.info(
                "ETL completed for ticker",
                ticker=ticker,
                loaded=loaded_count,
                status=stats["status"],
            )

        except Exception as e:
            logger.error(
                "ETL pipeline error",
                ticker=ticker,
                error=str(e),
            )
            stats["status"] = "error"
            stats["reason"] = str(e)

        return stats

    async def run_batch(
        self,
        tickers: list[str] | None = None,
        force: bool = False,
        incremental: bool = True,
    ) -> BatchStats:
        """
        Run ETL pipeline for multiple tickers with smart incremental loading.

        Args:
            tickers: List of ticker symbols. If None, uses default from config.
            force: Force ETL even if data is current
            incremental: Only load new data (recommended for scheduled jobs)

        Returns:
            Dictionary with batch execution statistics
        """
        if tickers is None:
            tickers = settings.ticker_list

        logger.info("Starting batch ETL", tickers=tickers, incremental=incremental, force=force)

        batch_stats: BatchStats = {
            "total_tickers": len(tickers),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "total_loaded": 0,
            "details": [],
        }

        for ticker in tickers:
            ticker_stats = await self.run_for_ticker(
                ticker, force=force, incremental=incremental
            )
            batch_stats["details"].append(ticker_stats)

            if ticker_stats["status"] == "success":
                batch_stats["successful"] += 1
                batch_stats["total_loaded"] += ticker_stats["loaded"]
            elif ticker_stats["status"] == "skipped":
                batch_stats["skipped"] += 1
            else:
                batch_stats["failed"] += 1

        logger.info(
            "Batch ETL completed",
            total=batch_stats["total_tickers"],
            successful=batch_stats["successful"],
            skipped=batch_stats["skipped"],
            failed=batch_stats["failed"],
            total_loaded=batch_stats["total_loaded"],
        )

        return batch_stats
