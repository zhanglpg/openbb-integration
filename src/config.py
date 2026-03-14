"""Configuration for OpenBB data pipeline."""

from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = DATA_DIR / "openbb_data.db"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

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

# Data refresh intervals (in hours)
REFRESH_INTERVALS = {
    "prices": 1,  # Hourly during market hours
    "fundamentals": 24,  # Daily
    "sec_filings": 6,  # Every 6 hours
    "economic": 24,  # Daily
}

# Alert thresholds
ALERT_THRESHOLDS = {
    "stock_daily_change": 0.05,  # 5%
    "etf_daily_change": 0.03,  # 3%
    "china_exposure_change": 0.04,  # 4%
}
