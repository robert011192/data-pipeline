from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # Application
    APP_NAME: str = "Market Data Pipeline"
    APP_VERSION: str = "1.0.0"
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"

    # API Configuration
    API_V1_PREFIX: str = "/api/v1"
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Database
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5433/market_data"
    )
    DATABASE_ECHO: bool = False

    # Alpha Vantage API
    ALPHA_VANTAGE_API_KEY: str = "demo"
    ALPHA_VANTAGE_BASE_URL: str = "https://www.alphavantage.co/query"

    # ETL Configuration
    ETL_ENABLED: bool = True
    ETL_INTERVAL_MINUTES: int = 5
    ETL_BATCH_SIZE: int = 100
    DEFAULT_TICKERS: str = "AAPL,GOOGL,MSFT,AMZN,TSLA"

    # CORS
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:8000"]

    # Pagination
    DEFAULT_PAGE_SIZE: int = 50
    MAX_PAGE_SIZE: int = 1000

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: str | list[str]) -> list[str] | str:
        """Parse CORS origins from string or list."""
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, list):
            return v
        return v

    @property
    def database_url_str(self) -> str:
        """Get database URL as string."""
        return str(self.DATABASE_URL)

    @property
    def ticker_list(self) -> list[str]:
        """Get list of tickers from comma-separated string."""
        return [ticker.strip() for ticker in self.DEFAULT_TICKERS.split(",")]


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Global settings instance
settings = get_settings()
