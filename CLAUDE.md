# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Financial data pipeline and portfolio dashboard built on the OpenBB SDK. Fetches equity prices, fundamentals, SEC filings, and economic indicators (FRED), stores them in SQLite + Parquet, and visualizes via a multi-page Streamlit dashboard.

## Commands

```bash
# Activate environment
cd ~/.openbb_platform && source .venv/bin/activate

# Run full pipeline (prices + fundamentals + SEC + economic)
python src/run_pipeline.py full

# Run individual pipeline stages
python src/run_pipeline.py prices
python src/run_pipeline.py fundamentals
python src/run_pipeline.py sec
python src/run_pipeline.py economic

# Quick smoke test (fetches one symbol)
python src/run_pipeline.py test

# Launch Streamlit dashboard
streamlit run dashboard.py

# Run all tests
pytest tests/

# Run a single test file / single test
pytest tests/test_database.py
pytest tests/test_database.py::test_save_prices -xvs

# Run by marker
pytest -m unit
pytest -m integration

# Lint
ruff check .
ruff format --check .
```

## Architecture

```
pipeline.py / src/run_pipeline.py   ← CLI entry points
    │
    ├── src/watchlist_fetcher.py     ← orchestrates per-symbol fetching
    │       └── src/fetcher.py       ← thin wrapper around OpenBB SDK (obb.equity.*)
    │       └── src/retry.py         ← exponential backoff for API calls
    │
    ├── src/sec_parser.py            ← SEC EDGAR filings (obb.equity.fundamental.filings)
    ├── src/economic_dashboard.py    ← FRED / macro indicators (obb.economy.*)
    │
    └── src/database.py              ← SQLite storage (schema v2, upsert-based)
    │   └── src/config.py            ← paths, watchlist dict, pipeline defaults
    │
    └── src/analysis.py              ← pure analysis functions (technicals, risk, macro)

dashboard.py                         ← Streamlit main page (Portfolio)
pages/2_Economy.py                   ← Streamlit economy page
shared.py                            ← shared Streamlit helpers (get_db, sidebar)

src/mcp_server.py                    ← MCP server (FastMCP) — 6 data tools + 5 analysis tools
```

### Key patterns

- **sys.path manipulation**: All top-level scripts (`pipeline.py`, `dashboard.py`, `shared.py`) insert `src/` into `sys.path` so `src/` modules are imported by bare name (e.g., `from database import Database`).
- **OpenBB SDK**: Data fetching uses `from openbb import obb`. Results may be OBBject (call `.to_dataframe()`) or raw DataFrames. Column names from OpenBB vary by provider and must be normalized to the DB schema.
- **Database schema**: `src/database.py` defines 6 tables (`price_history`, `fundamentals`, `sec_filings`, `economic_indicators`, `watchlist`, `fetch_log`). Schema versioned via `schema_version` table. All writes use `INSERT OR REPLACE` for idempotent re-runs.
- **Config-driven watchlist**: `src/config.py` defines `WATCHLIST` dict (categories → symbol lists) and `ECONOMIC_INDICATORS` (FRED series). The dashboard reads these directly; the pipeline uses `WatchlistManager` which can also read `data/watchlist.txt`.
- **Streamlit caching**: Dashboard uses `@st.cache_resource` for DB connection and `@st.cache_data(ttl=...)` for query results. Prefixing DB arg with `_` avoids Streamlit's hashing.

### Testing conventions

- Tests mock `openbb` at module level in `conftest.py` (`sys.modules.setdefault("openbb", ...)`) so tests run without the OpenBB SDK installed.
- `tmp_db` fixture provides a fresh SQLite database per test via `tmp_path`.
- Markers: `@pytest.mark.unit`, `@pytest.mark.integration`.

## Data storage

- **SQLite**: `~/.openbb_platform/data/openbb_data.db`
- **Parquet**: `data/prices/`, `data/fundamentals/`, `data/sec/` (written by `src/storage.py`)
- **Schema reset**: Delete `data/openbb_data.db` to recreate from scratch; pipeline is additive.

## API keys

Configured in `~/.openbb_platform/user_settings.json` under `credentials`. Yahoo Finance and SEC require no keys. FRED requires `fred_api_key`.
