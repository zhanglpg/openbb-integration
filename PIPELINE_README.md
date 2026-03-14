# OpenBB Data Pipeline

Daily data pipeline for fetching and storing financial data.

## Quick Start

```bash
# Activate virtual environment
cd /Users/lipingzhang/.openclaw/workspace/projects/openbb
source .venv/bin/activate

# Run the pipeline
python pipeline.py
```

## What It Does

1. **Loads watchlist** from `data/watchlist.txt`
2. **Fetches daily prices** (OHLCV) from Yahoo Finance
3. **Fetches fundamentals** (income statements, key metrics)
4. **Fetches SEC filings** (10-K, 10-Q only)
5. **Stores data** in:
   - Parquet files: `data/prices/`, `data/fundamentals/`, `data/sec/`
   - SQLite database: `data/metadata.db`

## Output

```
data/
├── metadata.db           # SQLite database with fetch logs, filings metadata
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
import pandas as pd
from src.storage import DataStorage

storage = DataStorage()

# Load prices
aapl_prices = storage.load_prices('AAPL')
print(aapl_prices.tail())

# Load fundamentals
aapl_metrics = storage.load_fundamentals('AAPL', 'metrics')
print(aapl_metrics)

# Load SEC filings
aapl_filings = pd.read_parquet(storage.sec_dir / 'AAPL_sec_filings_20260314.parquet')
print(aapl_filings[['filing_date', 'report_type', 'report_url']].head())
```

## Scheduling

Run daily with cron:
```bash
# Edit crontab
crontab -e

# Add daily job at 9 AM
0 9 * * * cd /Users/lipingzhang/.openclaw/workspace/projects/openbb && source .venv/bin/activate && python pipeline.py >> logs/pipeline.log 2>&1
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
