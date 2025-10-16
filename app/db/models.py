"""Database models for market data."""

from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, TimestampMixin


class MarketData(Base, TimestampMixin):
    """Model for storing stock market data."""

    __tablename__ = "market_data"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    open: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    volume: Mapped[int] = mapped_column(nullable=False)

    __table_args__ = (
        # Ensure one record per ticker per date
        UniqueConstraint("ticker", "date", name="uq_ticker_date"),
        # Composite index for common queries
        Index("ix_ticker_date", "ticker", "date"),
    )

    def __repr__(self) -> str:
        """String representation of the model."""
        return (
            f"<MarketData(ticker={self.ticker}, date={self.date}, "
            f"close={self.close}, volume={self.volume})>"
        )
