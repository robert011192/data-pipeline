# Market data pipeline

### Components

1. **ETL Pipeline**
   - **Extractor**: Fetches stock price data from Alpha Vantage API
   - **Transformer**: Validates and transforms raw API data into structured format
   - **Loader**: Efficiently loads data into PostgreSQL using upsert (INSERT ON CONFLICT)
   - **Scheduler**: Runs ETL jobs automatically at configurable intervals (default: 5 minutes)

2. **REST API**
   - Built with FastAPI for high performance and automatic OpenAPI documentation
   - Full CRUD operations on market data
   - Advanced filtering by ticker, date range
   - Pagination support
   - Async/await for concurrent request handling
   - Structured logging with contextual information

3. **Database**
   - PostgreSQL with asyncpg driver for async operations
   - Optimized indexes for common query patterns
   - Unique constraints to prevent duplicate data
   - Timestamp tracking (created_at, updated_at)

## Project Structure

```
data-pipeline/
├── app/
│   ├── api/
│   │   ├── deps.py              # Dependency injection
│   │   └── v1/
│   │       ├── router.py         # API router
│   │       └── endpoints/
│   │           └── market_data.py # CRUD endpoints
│   ├── core/
│   │   ├── config.py            # Settings management
│   │   └── logging.py           # Logging configuration
│   ├── db/
│   │   ├── base.py              # SQLAlchemy base
│   │   ├── models.py            # Database models
│   │   └── session.py           # Database session
│   ├── etl/
│   │   ├── extractor.py         # Data extraction
│   │   ├── transformer.py       # Data transformation
│   │   ├── loader.py            # Data loading
│   │   └── pipeline.py          # Pipeline orchestration
│   ├── schemas/
│   │   └── market_data.py       # Pydantic schemas
│   ├── services/
│   │   └── market_data_service.py # Business logic
│   └── main.py                  # FastAPI application
├── tests/
│   ├── test_api/
│   │   └── test_market_data.py  # API tests
│   └── test_etl/
│       └── test_transformer.py  # ETL tests
├── alembic/                     # Database migrations
├── docker-compose.yml           # Docker orchestration
├── Dockerfile                   # Container definition
├── requirements.txt             # Python dependencies
├── pyproject.toml              # Project configuration
└── README.md                    # This file
```

## Getting started

### Prerequisites

- Python 3.12+
- Docker and Docker Compose
- PostgreSQL 16+ (if running locally without Docker)
- Alpha Vantage API key (free tier available at https://www.alphavantage.co/support/#api-key)

### Installation

#### Using Docker (Recommended)

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd data-pipeline
   ```

2. Copy environment file and configure:
   ```bash
   cp .env.example .env
   ```

3. Edit `.env` and add your Alpha Vantage API key:
   ```bash
   ALPHA_VANTAGE_API_KEY=your_api_key_here
   ```

4. Start the services:
   ```bash
   docker-compose up -d
   ```

5. Access the API:
   - API Documentation: http://localhost:8000/docs
   - API ReDoc: http://localhost:8000/redoc
   - Health Check: http://localhost:8000/api/v1/health

### Configuration

The application is configured via environment variables. Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:postgres@localhost:5432/market_data` | Database connection URL |
| `ALPHA_VANTAGE_API_KEY` | `demo` | Alpha Vantage API key |
| `ETL_ENABLED` | `True` | Enable automatic ETL scheduling |
| `ETL_INTERVAL_MINUTES` | `5` | Minutes between ETL runs |
| `DEFAULT_TICKERS` | `AAPL,GOOGL,MSFT,AMZN,TSLA` | Comma-separated list of default tickers |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `DEFAULT_PAGE_SIZE` | `50` | Default page size for list endpoints |
| `MAX_PAGE_SIZE` | `1000` | Maximum allowed page size |

## Testing

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=app --cov-report=html --cov-report=term-missing
```

### Run specific test file
```bash
pytest tests/test_api/test_market_data.py -v
```

## Design decisions & trade-offs

### 1. Async/Await architecture
**Decision**: Use async/await throughout the application with asyncpg and httpx.

**Rationale**:
- Non-blocking I/O allows handling multiple concurrent requests efficiently
- Better resource utilization for I/O-bound operations (API calls, database queries)
- FastAPI's native async support provides optimal performance

**Trade-off**: Slightly more complex code compared to synchronous approach, but significant performance benefits.

### 2. Upsert strategy
**Decision**: Use PostgreSQL's `INSERT ... ON CONFLICT ... DO UPDATE` for data loading.

**Rationale**:
- Handles duplicate data gracefully (idempotent operations)
- Single database round-trip per batch
- Automatically updates existing records with new data

**Trade-off**: PostgreSQL-specific syntax (not portable to other databases without modification).

### 3. Scheduled ETL with APScheduler
**Decision**: Use APScheduler for periodic ETL jobs rather than external orchestration.

**Rationale**:
- Simplifies deployment (no external dependencies like Airflow, Celery)
- Sufficient for small-to-medium scale workloads
- Easy configuration via environment variables

**Trade-off**: Limited scalability compared to dedicated orchestration tools. For production at scale, consider migrating to Airflow or similar.

### 4. Alpha Vantage API rate limiting
**Decision**: Sequential API calls with error handling.

**Rationale**:
- Alpha Vantage free tier has rate limits (5 API calls per minute, 500 per day)
- Sequential approach prevents rate limit violations
- Error handling logs failures without crashing the pipeline

**Trade-off**: Slower than parallel requests, but respects API constraints. Consider upgrading to premium API for higher throughput.

## Monitoring & observability

### Health checks
- `/api/v1/health` - Overall health including database connectivity

### Logging
- Structured JSON logs in production
- Pretty console logs in development
- Contextual information (ticker, date, record_id) included in logs
- Different log levels for different environments
