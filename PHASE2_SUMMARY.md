# Phase 2 Summary — Watchlist Data Pipeline

**Status:** ✅ Sprint 1 Complete (Watchlist Fetcher)  
**Date:** 2026-03-14  
**Owner:** Finance Agent

---

## What Was Built

### Code (5 modules, ~800 lines)

| File | Purpose |
|------|---------|
| `src/watchlist.py` | Symbol management (load, add, remove) |
| `src/fetcher.py` | OpenBB SDK wrapper for prices, fundamentals, filings |
| `src/storage.py` | SQLite + Parquet storage layer |
| `pipeline.py` | Daily orchestration script |
| `query.py` | Data access demo |

### Data Fetched (10 symbols)

| Data Type | Records | Storage |
|-----------|---------|---------|
| Historical Prices | 2,510 rows (251 per symbol) | `data/prices/` |
| Fundamentals | 46 records (income + metrics) | `data/fundamentals/` |
| SEC Filings | 268 filings (10-K/10-Q) | `data/filings/` |
| **Total** | — | **~1.5MB** |

### Watchlist

```
AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, JPM, V, WMT
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Watchlist Manager                         │
│  data/watchlist.txt → 10 symbols                            │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Data Fetcher (OpenBB SDK)                  │
│  • Prices: obb.equity.price.historical()                   │
│  • Fundamentals: obb.equity.fundamental.{income,metrics}() │
│  • SEC Filings: obb.equity.fundamental.filings()           │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                  Storage Layer                               │
│  • Parquet: Time-series data (prices, fundamentals)         │
│  • SQLite: Metadata, fetch logs, filings index              │
└─────────────────────────────────────────────────────────────┘
```

---

## Working API Routes (No Key Needed)

```python
# Historical prices (Yahoo Finance)
obb.equity.price.historical('AAPL', provider='yfinance')

# Fundamentals (Yahoo Finance)
obb.equity.fundamental.income('AAPL', provider='yfinance')
obb.equity.fundamental.metrics('AAPL', provider='yfinance')

# SEC Filings (SEC EDGAR)
obb.equity.fundamental.filings('AAPL', provider='sec')
```

---

## Blockers

### 🔴 FRED API Key Required

| Item | Details |
|------|---------|
| **URL** | https://fred.stlouisfed.org/ |
| **Setup time** | 5 minutes |
| **Impact** | Economic indicators dashboard blocked |
| **Action** | Liping needs to obtain key |

**Routes blocked:**
```python
obb.economy.fred_series('GDP')
obb.economy.fred_series('CPIAUCSL')
obb.economy.fred_series('UNRATE')
```

---

## Usage

```bash
# Navigate to project
cd ~/.openbb_platform

# Activate environment
source .venv/bin/activate

# Run daily pipeline
python pipeline.py

# Query data
python query.py
```

---

## Next Sprints

### Sprint 2: SEC XBRL Parsing
- Parse XBRL data from 10-K/10-Q filings
- Extract Revenue, Net Income, EPS
- Store in structured format

### Sprint 3: Economic Dashboard
- **BLOCKED** until FRED API key obtained
- Fetch GDP, CPI, unemployment rate
- Build visualization dashboard

### Sprint 4: Automation
- Set up cron job for daily execution (7 AM)
- Add error handling & notifications
- Final documentation

---

## Files to Review

| File | Description |
|------|-------------|
| `PIPELINE_README.md` | How to use the pipeline |
| `PHASE2_SUMMARY.md` | This file — complete summary |
| `project.md` | Updated project status |

---

**Status:** ✅ Ready for review. FRED API key needed for Sprint 3.
