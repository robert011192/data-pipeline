from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi_pagination import Page
from fastapi_pagination.ext.sqlalchemy import paginate
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db
from app.core.config import settings
from app.core.logging import get_logger
from app.etl.pipeline import ETLPipeline
from app.schemas.market_data import (
    HealthCheck,
    MarketDataCreate,
    MarketDataResponse,
    MarketDataUpdate,
)
from app.services.market_data_service import MarketDataService

logger = get_logger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthCheck)
async def health_check(db: AsyncSession = Depends(get_db)) -> HealthCheck:
    """
    Health check endpoint.

    Returns:
        Health status information
    """
    try:
        # Test database connection
        await db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        logger.error("Database health check failed", error=str(e))
        db_status = "unhealthy"

    return HealthCheck(
        status="healthy" if db_status == "healthy" else "degraded",
        timestamp=datetime.now(),
        version=settings.APP_VERSION,
        database=db_status,
    )


@router.post(
    "/market-data",
    response_model=MarketDataResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_market_data(
    data: MarketDataCreate,
    db: AsyncSession = Depends(get_db),
) -> MarketDataResponse:
    """
    Create a new market data record.

    Args:
        data: Market data to create
        db: Database session

    Returns:
        Created market data

    Raises:
        HTTPException: If creation fails
    """
    service = MarketDataService(db)

    try:
        existing = await service.get_by_ticker_and_date(data.ticker, data.date_)
        if existing:
            raise HTTPException(  # noqa: B904
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Market data for {data.ticker} on {data.date_} already exists",
            )

        return await service.create(data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error creating market data", error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create market data",
        )


@router.get("/market-data", response_model=Page[MarketDataResponse])
async def list_market_data(
    ticker: str | None = Query(None, description="Filter by ticker symbol"),
    start_date: date | None = Query(None, description="Start date (inclusive)"),
    end_date: date | None = Query(None, description="End date (inclusive)"),
    db: AsyncSession = Depends(get_db),
) -> Page[MarketDataResponse]:
    """
    List market data with optional filters and pagination.

    Pagination is handled automatically via query parameters:
    - page: Page number (default: 1)
    - size: Items per page (default: 50, max: 100)

    Args:
        ticker: Filter by ticker symbol
        start_date: Filter by start date
        end_date: Filter by end date
        db: Database session

    Returns:
        Paginated list of market data
    """
    service = MarketDataService(db)

    try:
        query = service.build_query(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )
        return await paginate(db, query)  # type: ignore[no-any-return]
    except Exception as e:
        logger.error("Error listing market data", error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list market data",
        )


@router.get("/market-data/{record_id}", response_model=MarketDataResponse)
async def get_market_data(
    record_id: int,
    db: AsyncSession = Depends(get_db),
) -> MarketDataResponse:
    """
    Get market data by ID.

    Args:
        record_id: Record ID
        db: Database session

    Returns:
        Market data record

    Raises:
        HTTPException: If record not found
    """
    service = MarketDataService(db)

    try:
        data = await service.get_by_id(record_id)
        if not data:
            raise HTTPException(  # noqa: B904
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Market data with ID {record_id} not found",
            )
        return data
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting market data", record_id=record_id, error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get market data",
        )


@router.put("/market-data/{record_id}", response_model=MarketDataResponse)
async def update_market_data(
    record_id: int,
    data: MarketDataUpdate,
    db: AsyncSession = Depends(get_db),
) -> MarketDataResponse:
    """
    Update market data record.

    Args:
        record_id: Record ID
        data: Data to update
        db: Database session

    Returns:
        Updated market data

    Raises:
        HTTPException: If record not found
    """
    service = MarketDataService(db)

    try:
        updated = await service.update(record_id, data)
        if not updated:
            raise HTTPException(  # noqa: B904
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Market data with ID {record_id} not found",
            )
        return updated
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error updating market data", record_id=record_id, error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update market data",
        )


@router.delete("/market-data/{record_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_market_data(
    record_id: int,
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete market data record.

    Args:
        record_id: Record ID
        db: Database session

    Raises:
        HTTPException: If record not found
    """
    service = MarketDataService(db)

    try:
        deleted = await service.delete(record_id)
        if not deleted:
            raise HTTPException(  # noqa: B904
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Market data with ID {record_id} not found",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error deleting market data", record_id=record_id, error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete market data",
        )


@router.get("/tickers", response_model=list[str])
async def list_tickers(db: AsyncSession = Depends(get_db)) -> list[str]:
    """
    Get list of unique tickers in database.

    Args:
        db: Database session

    Returns:
        List of ticker symbols
    """
    service = MarketDataService(db)

    try:
        return await service.get_tickers()
    except Exception as e:
        logger.error("Error listing tickers", error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list tickers",
        )


@router.post("/etl/run")
async def trigger_etl(
    tickers: list[str] | None = Query(None, description="List of tickers to process"),
    force: bool = Query(False, description="Force ETL even if data is current"),
    incremental: bool = Query(
        False, description="Only load new data (skip if data is up-to-date)"
    ),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    Manually trigger ETL pipeline.

    Args:
        tickers: Optional list of tickers. If not provided, uses default tickers.
        force: Force ETL to run even if data is already current
        incremental: Enable smart incremental loading (skips if data is current)
        db: Database session

    Returns:
        ETL execution statistics

    Examples:
        - Full refresh: POST /etl/run?force=true&incremental=false
        - Incremental (smart): POST /etl/run?incremental=true
        - Force incremental: POST /etl/run?force=true&incremental=true
    """
    try:
        pipeline = ETLPipeline(db)
        stats = await pipeline.run_batch(tickers, force=force, incremental=incremental)

        logger.info("ETL triggered manually", stats=stats, force=force, incremental=incremental)

        return stats
    except Exception as e:
        logger.error("Error running ETL", error=str(e))
        raise HTTPException(  # noqa: B904
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to run ETL pipeline",
        )
