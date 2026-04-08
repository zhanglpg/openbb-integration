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

The dashboard has **5 pages**:

1. **Portfolio** (`dashboard.py`) — Latest prices, daily changes, drag-drop symbol reordering, portfolio summary
2. **Economy** (`pages/2_Economy.py`) — FRED macro indicators (GDP, CPI, unemployment, Fed funds rate, yield curves, etc.)
3. **Reports** (`pages/3_Reports.py`) — Daily briefs, SEC filing summaries, valuation screens
4. **Research** (`pages/4_Research.py`) — Deep analysis tools including:
   - Company research reports
   - Peer comparison analysis
   - Industry trend analysis
   - Risk assessment
   - Earnings analysis
5. **Charts** (`pages/5_Charts.py`) — Interactive price charts, technical indicators, correlation matrices

The dashboard can also be managed as a background service via launchctl (`~/Library/LaunchAgents/com.openclaw.dashboard.plist`).

### MCP Server (Claude Code Integration)

An MCP server (`src/mcp_server.py`) exposes the database as tools for Claude Code:

**Data tools:**
- `get_portfolio_overview` — Latest prices and daily change for all symbols
- `get_price_history` — OHLCV history for a symbol
- `get_fundamentals` — PE, market cap, EPS, revenue, etc.
- `get_sec_filings` — Recent SEC filings
- `get_economic_indicators` — FRED series data
- `get_watchlist` — Current watchlist configuration

**Analysis tools:**
- `analyze_price_technicals` — Moving averages, RSI, MACD for a symbol
- `screen_valuations` — Sort and filter by valuation metrics
- `get_portfolio_risk_summary` — VaR, Sharpe, beta, correlations
- `get_macro_snapshot` — Economic indicator trends
- `get_sec_activity_summary` — Filing frequency and patterns

**Report tools:**
- `get_daily_report` — Rendered daily brief for a given date
- `list_reports` — Available daily report files

**Research tools:**
- `compare_sector_peers` — Side-by-side category comparison
- `deep_analyze_symbol` — Comprehensive single-symbol analysis
- `assess_portfolio_risks` — Portfolio-wide risk assessment
- `find_opportunities` — Screen for undervalued symbols

Configured via `.mcp.json` in the project root.

### Daily Brief Export

The `brief_exporter.py` module exports portfolio data as structured JSON for the brief generator, including:
- Portfolio performance
- Key market movements
- SEC filing activity
- Economic indicator updates

```bash
python src/brief_exporter.py --output /path/to/brief.json
```

## Architecture

```
src/
├── run_pipeline.py          ← CLI entry point
├── watchlist_fetcher.py     ← Orchestrates per-symbol fetching
│   ├── fetcher.py           ← Wrapper around OpenBB SDK (obb.equity.*)
│   └── retry.py             ← Exponential backoff for API calls
├── sec_parser.py            ← SEC EDGAR filings parser
├── economic_dashboard.py    ← FRED / macro indicators
├── database.py              ← SQLite storage (schema v3, upsert-based)
├── storage.py               ← Parquet file storage
├── config.py                ← Paths, watchlist, pipeline defaults
├── analysis.py              ← Portfolio risk, technicals, valuation screens
├── report.py                ← Report generation and formatting
├── research.py              ← Deep research and peer analysis
├── brief_exporter.py        ← Daily brief markdown export
├── watchlist.py             ← Watchlist management
└── mcp_server.py            ← MCP server for Claude Code

dashboard.py                 ← Streamlit main page (Portfolio)
pages/
├── 2_Economy.py             ← Economy page (FRED indicators)
├── 3_Reports.py             ← Reports page (briefs, filings, screens)
├── 4_Research.py            ← Research page (deep analysis)
└── 5_Charts.py              ← Charts page (visualization)

shared.py                    ← Shared Streamlit helpers
pipeline.py                  ← Legacy CLI entry point (wraps run_pipeline)
query.py                     ← Quick data query utility
```

## Data Storage

- **SQLite**: `data/openbb_data.db` — 8 tables (`price_history`, `fundamentals`, `sec_filings`, `economic_indicators`, `watchlist`, `fetch_log`, `holdings`, `research_notes`)
- **Parquet**: `data/prices/`, `data/fundamentals/`, `data/sec/`
- **Reports**: `data/reports/` — Generated daily brief markdown files
- Schema is versioned; delete the DB file to recreate from scratch (pipeline is additive/idempotent)

## Analysis Features

The `analysis.py` module provides:

- **Portfolio Risk Metrics** — VaR, Sharpe ratio, beta, correlation analysis
- **Price Technicals** — Moving averages, RSI, MACD, Bollinger Bands
- **Valuation Screens** — PE, PB, PEG, EV/EBITDA comparisons
- **SEC Activity Analysis** — Filing frequency, insider trading patterns
- **Macro Snapshot** — Economic indicator trends and comparisons

## Configuration

Edit `src/config.py` to customize:

- **WATCHLIST** — Symbols to track (organized by category)
- **ECONOMIC_INDICATORS** — FRED series IDs to fetch
- **DB_PATH** — SQLite database location
- **REPORTS_DIR** — Output directory for generated reports

## Development

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Lint
ruff check .
ruff format --check .
```

## License

MIT
