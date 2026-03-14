# Phase 2: Data Pipeline Development

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Watchlist Manager                         │
│  - symbols.txt (AAPL, MSFT, GOOGL, TSLA, NVDA, ...)         │
│  - Daily scheduling (cron/asyncio)                          │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Data Fetcher Layer                         │
│  - Historical prices (daily OHLCV)                          │
│  - Fundamentals (income, balance, cash flow)                │
│  - Metrics (P/E, market cap, ratios)                        │
│  - SEC filings (10-K, 10-Q, 8-K)                            │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  Storage Layer                               │
│  - SQLite: Metadata, watchlist, schedules                   │
│  - Parquet: Time-series data (prices, fundamentals)         │
│  - Directory structure: data/{symbol}/{type}/{date}.parquet │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  Query/Analysis Layer                        │
│  - Load historical data from parquet                        │
│  - Compute custom metrics                                   │
│  - Export to pandas for analysis                            │
└─────────────────────────────────────────────────────────────┘
```

## API Routes Discovered

### 1. Historical Prices
```python
obb.equity.price.historical('AAPL', provider='yfinance')
```
- Provider: `yfinance` (free, no key needed)
- Returns: OHLCV data with dates

### 2. Fundamentals
```python
# Income Statement
obb.equity.fundamental.income('AAPL', provider='yfinance')

# Balance Sheet
obb.equity.fundamental.balance('AAPL', provider='yfinance')

# Cash Flow
obb.equity.fundamental.cash('AAPL', provider='yfinance')

# Key Metrics
obb.equity.fundamental.metrics('AAPL', provider='yfinance')
```
- Provider: `yfinance` (free), `fmp` (requires API key), `intrinio` (paid)

### 3. SEC Filings
```python
obb.equity.fundamental.filings('AAPL', provider='sec')
```
- Provider: `sec` (free, no key needed)
- Returns: All filings (10-K, 10-Q, 8-K, 3, 4, etc.)
- Columns: filing_date, report_type, report_url, accession_number, etc.

### 4. Economic Indicators (FRED) - Requires API Key
```python
obb.economy.fred_search('GDP')  # Search for indicators
obb.economy.fred_series('GDP')  # Get GDP data
obb.economy.fred_series('CPIAUCSL')  # CPI
obb.economy.fred_series('UNRATE')  # Unemployment rate
```
- Provider: `fred` (requires API key from fred.stlouisfed.org)

## Implementation Plan

### Sprint 1: Watchlist Data Fetcher (Days 1-3)
- [ ] Create `src/watchlist.py` - Watchlist management
- [ ] Create `src/fetcher.py` - Data fetching logic
- [ ] Create `src/storage.py` - SQLite + Parquet storage
- [ ] Create `data/watchlist.txt` - Default watchlist
- [ ] Test: Fetch daily prices for 5 symbols
- [ ] Test: Store in parquet format

### Sprint 2: SEC Filings Parser (Days 4-6)
- [ ] Create `src/sec_parser.py` - Parse SEC filings
- [ ] Extract 10-K/10-Q XBRL data
- [ ] Store parsed filings in SQLite
- [ ] Test: Parse AAPL 10-K filing

### Sprint 3: Economic Dashboard (Days 7-9)
- [ ] Get FRED API key from Liping
- [ ] Create `src/economy.py` - Economic indicators
- [ ] Build dashboard data structure
- [ ] Test: Fetch GDP, CPI, unemployment data

### Sprint 4: Integration & Testing (Days 10-14)
- [ ] Create daily fetch scheduler
- [ ] Build query interface
- [ ] Add error handling & logging
- [ ] Documentation

## File Structure
```
projects/openbb/
├── .venv/                    # Python virtual environment
├── src/                      # Source code (to create)
│   ├── __init__.py
│   ├── watchlist.py          # Watchlist management
│   ├── fetcher.py            # Data fetching
│   ├── storage.py            # SQLite + Parquet
│   ├── sec_parser.py         # SEC filings parser
│   └── economy.py            # Economic indicators
├── data/                     # Data storage (to create)
│   ├── watchlist.txt         # Symbol list
│   ├── prices/               # Parquet files
│   ├── fundamentals/         # Parquet files
│   └── sec/                  # Filing data
├── tests/                    # Tests (to create)
│   └── test_fetcher.py
├── config/                   # Configuration (to create)
│   └── settings.json
├── project.md                # This file
├── README.md                 # Project proposal
└── requirements.txt          # Dependencies
```

## API Keys Status

| Provider | Status | Action Needed |
|----------|--------|---------------|
| Yahoo Finance | ✅ Working | None |
| SEC EDGAR | ✅ Working | None |
| FRED | ⏳ Pending | Liping: Get API key |
| FMP | ⏳ Pending | Optional, free tier |

## Next Steps

1. **Create directory structure** - `src/`, `data/`, `tests/`, `config/`
2. **Build watchlist fetcher** - Start with price data
3. **Implement storage layer** - Parquet for time-series
4. **Test SEC filings** - Parse and store 10-K/10-Q
5. **Get FRED API key** - Unblock economic dashboard

## Blockers

- **FRED API key** - Needed for economic indicators dashboard
- **FMP API key** (optional) - Better fundamental data quality

---

*Last updated: 2026-03-14T08:05*
*Subagent: finance*
