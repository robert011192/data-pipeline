"""Data loading into database."""

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.consts import DB_CONFLICT_INDEX_ELEMENTS
from app.core.logging import get_logger
from app.db.models import MarketData
from app.schemas.market_data import MarketDataCreate

logger = get_logger(__name__)


class DataLoader:
    """Load transformed data into the database."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the data loader.

        Args:
            session: Database session
        """
        self.session = session

    async def load_market_data(self, data: MarketDataCreate) -> MarketData | None:
        """
        Load a single market data record into the database.

        Uses PostgreSQL's ON CONFLICT to handle duplicates (upsert).

        Args:
            data: Market data to load

        Returns:
            Created or updated MarketData object, or None if failed
        """
        try:
            # Use PostgreSQL's INSERT ... ON CONFLICT ... DO UPDATE
            stmt = insert(MarketData).values(
                ticker=data.ticker,
                date=data.date_,
                open=data.open_price,
                high=data.high_price,
                low=data.low_price,
                close=data.close_price,
                volume=data.volume,
            )

            # Update if conflict on unique constraint (ticker, date)
            stmt = stmt.on_conflict_do_update(
                index_elements=DB_CONFLICT_INDEX_ELEMENTS,
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                },
            )

            await self.session.execute(stmt)
            await self.session.commit()

            # Fetch the record to return
            query = select(MarketData).where(
                MarketData.ticker == data.ticker, MarketData.date == data.date_
            )
            result = await self.session.execute(query)
            market_data = result.scalar_one_or_none()

            if market_data:
                logger.debug(
                    "Loaded market data",
                    ticker=data.ticker,
                    date=data.date_,
                )

            return market_data

        except Exception as e:
            await self.session.rollback()
            logger.error(
                "Error loading market data",
                ticker=data.ticker,
                date=data.date_,
                error=str(e),
            )
            return None

    async def load_batch(self, data_list: list[MarketDataCreate]) -> int:
        """
        Load multiple market data records in batch.

        Args:
            data_list: List of market data to load

        Returns:
            Number of successfully loaded records
        """
        if not data_list:
            return 0

        loaded_count = 0

        try:
            # Prepare batch insert with ON CONFLICT
            values = [
                {
                    "ticker": data.ticker,
                    "date": data.date_,
                    "open": data.open_price,
                    "high": data.high_price,
                    "low": data.low_price,
                    "close": data.close_price,
                    "volume": data.volume,
                }
                for data in data_list
            ]

            stmt = insert(MarketData).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=DB_CONFLICT_INDEX_ELEMENTS,
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                },
            )

            await self.session.execute(stmt)
            await self.session.commit()

            loaded_count = len(data_list)

            logger.info(
                "Batch load completed",
                records_loaded=loaded_count,
            )

        except Exception as e:
            await self.session.rollback()
            logger.error(
                "Error during batch load",
                batch_size=len(data_list),
                error=str(e),
            )

        return loaded_count

    async def get_latest_date_for_ticker(self, ticker: str) -> str | None:
        """
        Get the latest date for which data exists for a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Latest date as string, or None if no data exists
        """
        try:
            query = (
                select(MarketData.date)
                .where(MarketData.ticker == ticker)
                .order_by(MarketData.date.desc())
                .limit(1)
            )

            result = await self.session.execute(query)
            latest_date = result.scalar_one_or_none()

            return str(latest_date) if latest_date else None

        except Exception as e:
            logger.error(
                "Error fetching latest date",
                ticker=ticker,
                error=str(e),
            )
            return None
