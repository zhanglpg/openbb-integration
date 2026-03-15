# OpenClaw Finance

Financial data pipeline and portfolio dashboard built on the [OpenBB SDK](https://github.com/OpenBB-finance/OpenBB). Fetches equity prices, fundamentals, SEC filings, and economic indicators (FRED), stores them locally in SQLite + Parquet, and visualizes everything through a multi-page Streamlit dashboard.

## Watchlist

| Category | Symbols |
|----------|---------|
| Tech | AAPL, MSFT, GOOGL, NVDA, META |
| China | BABA, JD, PDD, TCEHY, BIDU |
| Semiconductors | NVDA, TSM, AMD, INTC, AVGO |
| ETFs | SPY, QQQ, FXI, KWEB, ARKK |

## Setup

```bash
cd ~/.openbb_platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

API keys are configured in `~/.openbb_platform/user_settings.json` under `credentials`. Yahoo Finance and SEC require no keys. FRED requires `fred_api_key`.

## Usage

### Data Pipeline

```bash
# Run full pipeline (prices + fundamentals + SEC + economic)
python src/run_pipeline.py full

# Run individual stages
python src/run_pipeline.py prices
python src/run_pipeline.py fundamentals
python src/run_pipeline.py sec
python src/run_pipeline.py economic

# Quick smoke test (fetches one symbol)
python src/run_pipeline.py test
```

### Streamlit Dashboard

```bash
streamlit run dashboard.py
```

The dashboard is two pages:

- **Portfolio** (`dashboard.py`) — latest prices, daily changes, drag-drop symbol reordering
- **Economy** (`pages/2_Economy.py`) — FRED macro indicators (GDP, CPI, unemployment, Fed funds rate, etc.)

The dashboard can also be managed as a background service via launchctl (`~/Library/LaunchAgents/com.openclaw.dashboard.plist`).

### MCP Server (Claude Code Integration)

An MCP server (`src/mcp_server.py`) exposes the database as tools for Claude Code:

- `get_portfolio_overview` — latest prices and daily change for all symbols
- `get_price_history` — OHLCV history for a symbol
- `get_fundamentals` — PE, market cap, EPS, revenue, etc.
- `get_sec_filings` — recent SEC filings
- `get_economic_indicators` — FRED series data
- `get_watchlist` — current watchlist configuration

Configured via `.mcp.json` in the project root.

## Architecture

```
src/run_pipeline.py              ← CLI entry point
    ├── src/watchlist_fetcher.py  ← orchestrates per-symbol fetching
    │   ├── src/fetcher.py       ← wrapper around OpenBB SDK (obb.equity.*)
    │   └── src/retry.py         ← exponential backoff for API calls
    ├── src/sec_parser.py        ← SEC EDGAR filings
    ├── src/economic_dashboard.py ← FRED / macro indicators
    ├── src/database.py          ← SQLite storage (schema v2, upsert-based)
    └── src/config.py            ← paths, watchlist, pipeline defaults

dashboard.py                     ← Streamlit main page (Portfolio)
pages/2_Economy.py               ← Streamlit economy page
shared.py                        ← shared Streamlit helpers

src/mcp_server.py                ← MCP server for Claude Code
```

## Data Storage

- **SQLite**: `data/openbb_data.db` — 6 tables (`price_history`, `fundamentals`, `sec_filings`, `economic_indicators`, `watchlist`, `fetch_log`)
- **Parquet**: `data/prices/`, `data/fundamentals/`, `data/sec/`
- Schema is versioned; delete the DB file to recreate from scratch (pipeline is additive/idempotent)

## Development

```bash
# Run tests
pytest tests/

# Lint
ruff check .
ruff format --check .
```
