"""Business logic for market data operations."""

from __future__ import annotations

from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from app.core.logging import get_logger
from app.db.models import MarketData
from app.schemas.market_data import (
    MarketDataCreate,
    MarketDataResponse,
    MarketDataUpdate,
)

logger = get_logger(__name__)


class MarketDataService:
    """Service for market data operations."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the service.

        Args:
            session: Database session
        """
        self.session = session

    async def create(self, data: MarketDataCreate) -> MarketDataResponse:
        """
        Create a new market data record.

        Args:
            data: Market data to create

        Returns:
            Created market data
        """
        db_obj = MarketData(
            ticker=data.ticker,
            date=data.date_,
            open=data.open_price,
            high=data.high_price,
            low=data.low_price,
            close=data.close_price,
            volume=data.volume,
        )

        self.session.add(db_obj)
        await self.session.commit()
        await self.session.refresh(db_obj)

        logger.info("Created market data", ticker=data.ticker, date=data.date_)

        return MarketDataResponse.model_validate(db_obj)

    async def get_by_id(self, record_id: int) -> MarketDataResponse | None:
        """
        Get market data by ID.

        Args:
            record_id: Record ID

        Returns:
            Market data or None if not found
        """
        query = select(MarketData).where(MarketData.id == record_id)
        result = await self.session.execute(query)
        db_obj = result.scalar_one_or_none()

        if db_obj:
            return MarketDataResponse.model_validate(db_obj)
        return None

    async def get_by_ticker_and_date(
        self, ticker: str, trading_date: date
    ) -> MarketDataResponse | None:
        """
        Get market data by ticker and date.

        Args:
            ticker: Stock ticker symbol
            trading_date: Trading date

        Returns:
            Market data or None if not found
        """
        query = select(MarketData).where(
            MarketData.ticker == ticker.upper(), MarketData.date == trading_date
        )
        result = await self.session.execute(query)
        db_obj = result.scalar_one_or_none()

        if db_obj:
            return MarketDataResponse.model_validate(db_obj)
        return None

    def build_query(
        self,
        ticker: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Select[tuple[MarketData]]:
        """
        Build query with optional filters.

        Args:
            ticker: Filter by ticker symbol
            start_date: Filter by start date (inclusive)
            end_date: Filter by end date (inclusive)

        Returns:
            SQLAlchemy select query
        """
        query = select(MarketData)

        if ticker:
            query = query.where(MarketData.ticker == ticker.upper())
        if start_date:
            query = query.where(MarketData.date >= start_date)
        if end_date:
            query = query.where(MarketData.date <= end_date)

        # Apply default ordering
        query = query.order_by(MarketData.date.desc(), MarketData.ticker)

        return query

    async def update(self, record_id: int, data: MarketDataUpdate) -> MarketDataResponse | None:
        """
        Update market data record.

        Args:
            record_id: Record ID
            data: Data to update

        Returns:
            Updated market data or None if not found
        """
        query = select(MarketData).where(MarketData.id == record_id)
        result = await self.session.execute(query)
        db_obj = result.scalar_one_or_none()

        if not db_obj:
            return None

        # Update fields
        update_data = data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_obj, field, value)

        await self.session.commit()
        await self.session.refresh(db_obj)

        logger.info("Updated market data", record_id=record_id)

        return MarketDataResponse.model_validate(db_obj)

    async def delete(self, record_id: int) -> bool:
        """
        Delete market data record.

        Args:
            record_id: Record ID

        Returns:
            True if deleted, False if not found
        """
        query = select(MarketData).where(MarketData.id == record_id)
        result = await self.session.execute(query)
        db_obj = result.scalar_one_or_none()

        if not db_obj:
            return False

        await self.session.delete(db_obj)
        await self.session.commit()

        logger.info("Deleted market data", record_id=record_id)

        return True

    async def get_tickers(self) -> list[str]:
        """
        Get list of unique tickers in database.

        Returns:
            List of ticker symbols
        """
        query = select(MarketData.ticker).distinct().order_by(MarketData.ticker)
        result = await self.session.execute(query)
        tickers = result.scalars().all()

        return list(tickers)

    async def get_latest_date_for_ticker(self, ticker: str) -> date | None:
        """
        Get the most recent date we have data for a specific ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Latest date or None if no data exists
        """
        query = select(func.max(MarketData.date)).where(
            MarketData.ticker == ticker.upper()
        )
        result = await self.session.execute(query)
        latest_date = result.scalar_one_or_none()

        return latest_date
