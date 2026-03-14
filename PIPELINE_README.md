# OpenBB Data Pipeline

Daily data pipeline for fetching and storing financial data.

## Quick Start

```bash
# Activate virtual environment
cd ~/.openbb_platform
source .venv/bin/activate

# Run the pipeline
python pipeline.py
```

## Database Reset (Schema v2)

The database schema was overhauled in schema v2. If you have an older
`data/openbb_data.db` or `data/metadata.db`, delete them and let the
pipeline recreate from scratch:

```bash
rm -f data/openbb_data.db data/metadata.db
```

The pipeline accumulates data across runs (prices, fundamentals, filings,
economic indicators are all additive).  Future incompatible schema changes
will require a migration script — the `schema_version` table in the database
tracks which version is active.

## What It Does

1. **Loads watchlist** from `data/watchlist.txt`
2. **Fetches daily prices** (OHLCV) from Yahoo Finance
3. **Fetches fundamentals** (income statements, key metrics)
4. **Fetches SEC filings** (10-K, 10-Q only)
5. **Stores data** in:
   - Parquet files: `data/prices/`, `data/fundamentals/`, `data/sec/`
   - SQLite database: `data/openbb_data.db`

## Output

```
data/
├── openbb_data.db        # SQLite database (prices, fundamentals, filings, indicators)
├── watchlist.txt         # Symbol list
├── prices/
│   ├── AAPL_prices_20260314.parquet
│   ├── MSFT_prices_20260314.parquet
│   └── ...
├── fundamentals/
│   ├── AAPL_income_20260314.parquet
│   ├── AAPL_metrics_20260314.parquet
│   └── ...
└── sec/
    ├── AAPL_sec_filings_20260314.parquet
    ├── MSFT_sec_filings_20260314.parquet
    └── ...
```

## Customization

### Add Symbols

Edit `data/watchlist.txt`:
```
# One symbol per line
AAPL
MSFT
YOUR_SYMBOL
```

### Query Data

```python
from src.database import Database

db = Database()

# Load latest prices
aapl_prices = db.get_latest_prices('AAPL', days=30)
print(aapl_prices.tail())

# Batch query for multiple symbols
latest = db.get_latest_prices_batch(['AAPL', 'MSFT', 'GOOGL'])
print(latest)

# Economic indicators
indicators = db.get_latest_economic_indicators(['VIXCLS', 'DGS10'])
print(indicators)
```

## Scheduling

Run daily with cron:
```bash
# Edit crontab
crontab -e

# Add daily job at 9 AM
0 9 * * * cd /Users/lipingzhang/.openclaw/workspace/projects/openbb && source ~/.openbb_platform/.venv/bin/activate && python pipeline.py >> logs/pipeline.log 2>&1
```

## API Keys

| Provider | Required | Key Needed |
|----------|----------|------------|
| Yahoo Finance | No | Free, no key |
| SEC EDGAR | No | Free, no key |
| FRED | Yes | Get from https://fred.stlouisfed.org/ |
| FMP | Optional | Get from https://financialmodelingprep.com/ |

## Troubleshooting

**Error: `pyarrow` not found**
```bash
source .venv/bin/activate
pip install pyarrow
```

**Error: `No data returned`**
- Check symbol is correct (e.g., BRK-B not BRK.B)
- Some symbols may not be available on Yahoo Finance

---

*Built with OpenBB SDK v4.7.1*
