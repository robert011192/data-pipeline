from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type: ignore[import-untyped]
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_pagination import add_pagination

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.db.session import AsyncSessionLocal, close_db, init_db
from app.etl.pipeline import ETLPipeline

logger = get_logger(__name__)

scheduler = AsyncIOScheduler()


async def scheduled_etl_job() -> None:
    """
    Run ETL pipeline on schedule with incremental loading.

    This job runs periodically and skips tickers that already
    have current data, saving API calls and avoiding rate limits.
    """
    logger.info("Starting scheduled ETL job (incremental mode)")

    try:
        async with AsyncSessionLocal() as session:
            pipeline = ETLPipeline(session)
            stats = await pipeline.run_batch(incremental=True, force=False)
            logger.info("Scheduled ETL completed", stats=stats)
    except Exception as e:
        logger.error("Scheduled ETL failed", error=str(e))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Handles startup and shutdown events.
    """
    # Startup
    logger.info("Starting application", version=settings.APP_VERSION)

    try:
        # Initialize database
        await init_db()
        logger.info("Database initialized")

        # Start ETL scheduler if enabled
        if settings.ETL_ENABLED:
            scheduler.add_job(
                scheduled_etl_job,
                "interval",
                minutes=settings.ETL_INTERVAL_MINUTES,
                id="etl_job",
                replace_existing=True,
            )
            scheduler.start()
            logger.info(
                "ETL scheduler started",
                interval_minutes=settings.ETL_INTERVAL_MINUTES,
            )

            # Run initial ETL job
            await scheduled_etl_job()

    except Exception as e:
        logger.error("Startup failed", error=str(e))
        raise

    yield

    # Shutdown
    logger.info("Shutting down application")

    try:
        # Stop scheduler
        if settings.ETL_ENABLED and scheduler.running:
            scheduler.shutdown()
            logger.info("ETL scheduler stopped")

        # Close database connections
        await close_db()
        logger.info("Database connections closed")

    except Exception as e:
        logger.error("Shutdown error", error=str(e))


# Configure logging
configure_logging()

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="ETL pipeline and REST API for market data",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_PREFIX)

# Add pagination support
add_pagination(app)
