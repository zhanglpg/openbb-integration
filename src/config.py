"""Configuration for OpenBB data pipeline."""

from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = DATA_DIR / "openbb_data.db"
REPORTS_DIR = DATA_DIR / "reports"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Watchlist configuration
WATCHLIST = {
    "tech": ["AAPL", "MSFT", "GOOGL", "NVDA", "META"],
    "china": ["BABA", "JD", "PDD", "TCEHY", "BIDU"],
    "semiconductors": ["NVDA", "TSM", "AMD", "INTC", "AVGO"],
    "etfs": ["SPY", "QQQ", "FXI", "KWEB", "ARKK"],
}

# Economic indicators to track (FRED series - requires API key)
ECONOMIC_INDICATORS = {
    "GDP": "Gross Domestic Product",
    "UNRATE": "Unemployment Rate",
    "CPIAUCSL": "Consumer Price Index",
    "FEDFUNDS": "Federal Funds Rate",
    "T10Y2Y": "10-Year Treasury Minus 2-Year Treasury",
    "VIXCLS": "VIX Volatility Index",
    "DGS10": "10-Year Treasury Rate",
}

# SEC filing types to monitor
SEC_FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]

# Pipeline defaults — referenced by run_pipeline.py, watchlist_fetcher.py, dashboard.py
PIPELINE_DEFAULTS = {
    "price_lookback_days": 3650,
    "sec_filing_limit": 10,
    "api_call_delay": 0.5,  # seconds between consecutive API calls
    "max_retries": 3,
    "retry_backoff_base": 2,  # exponential backoff base in seconds
}
