"""Application constants."""

# Alpha Vantage API response keys
API_ERROR_MESSAGE_KEY = "Error Message"
API_NOTE_KEY = "Note"
API_INFORMATION_KEY = "Information"
API_TIME_SERIES_DAILY_KEY = "Time Series (Daily)"

# Alpha Vantage API function names
API_FUNCTION_TIME_SERIES_DAILY = "TIME_SERIES_DAILY"

# Alpha Vantage API output sizes
API_OUTPUT_SIZE_COMPACT = "compact"  # Last 100 data points
API_OUTPUT_SIZE_FULL = "full"  # Full historical data

# HTTP timeouts
HTTP_TIMEOUT_SECONDS = 30.0

# Database conflict handling
DB_CONFLICT_INDEX_ELEMENTS = ["ticker", "date"]

# Pagination
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100
