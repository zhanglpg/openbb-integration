"""MCP server exposing OpenClaw financial data (Tier 1 — read-only access)."""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
from fastmcp import FastMCP

# Ensure src/ is on the path so we can import config and database
sys.path.insert(0, str(Path(__file__).parent))

from analysis import (
    compute_macro_snapshot,
    compute_portfolio_risk,
    compute_price_technicals,
    compute_sec_activity,
    compute_valuation_screen,
)
from config import DB_PATH, ECONOMIC_INDICATORS, WATCHLIST
from database import Database

mcp = FastMCP("openclaw-finance")
db = Database()

# Flat list of all watchlist symbols (deduplicated, sorted)
ALL_SYMBOLS = sorted(set(s for symbols in WATCHLIST.values() for s in symbols))


# ------------------------------------------------------------------
# Resources
# ------------------------------------------------------------------


@mcp.resource("watchlist://symbols")
def watchlist_symbols() -> dict:
    """All tracked symbols grouped by category."""
    return WATCHLIST


# ------------------------------------------------------------------
# Tools
# ------------------------------------------------------------------


@mcp.tool()
def get_portfolio_overview() -> list[dict]:
    """Latest prices and daily change % for all watchlist symbols."""
    df = db.get_latest_prices_batch_with_previous(ALL_SYMBOLS)
    if df.empty:
        return []

    results = []
    # Build a symbol -> category mapping
    symbol_sector = {}
    for category, symbols in WATCHLIST.items():
        for s in symbols:
            if s not in symbol_sector:
                symbol_sector[s] = category

    for symbol in df["symbol"].unique():
        rows = df[df["symbol"] == symbol].sort_values("date", ascending=False)
        latest = rows.iloc[0]
        price = latest["close"]
        change_pct = None
        if len(rows) >= 2:
            prev_close = rows.iloc[1]["close"]
            if prev_close and prev_close != 0:
                change_pct = round((price - prev_close) / prev_close * 100, 2)

        results.append(
            {
                "symbol": symbol,
                "sector": symbol_sector.get(symbol, "unknown"),
                "date": latest["date"],
                "price": price,
                "change_pct": change_pct,
            }
        )

    return results


@mcp.tool()
def get_price_history(symbol: str, days: int = 90) -> list[dict]:
    """OHLCV price history for a symbol over the given number of days."""
    df = db.get_latest_prices(symbol.upper(), days)
    if df.empty:
        return []
    # Drop internal columns
    df = df.drop(columns=["id", "fetched_at"], errors="ignore")
    return df.to_dict(orient="records")


@mcp.tool()
def get_fundamentals(symbol: str) -> dict:
    """Latest fundamentals (PE, market cap, EPS, revenue, etc.) for a symbol."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM fundamentals WHERE symbol = ? ORDER BY snapshot_date DESC LIMIT 1",
            (symbol.upper(),),
        ).fetchone()
    if row is None:
        return {}
    result = dict(row)
    # Drop internal columns
    result.pop("id", None)
    result.pop("fetched_at", None)
    return result


@mcp.tool()
def get_sec_filings(symbol: str, limit: int = 10) -> list[dict]:
    """Recent SEC filings for a symbol."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM sec_filings WHERE symbol = ? ORDER BY filing_date DESC LIMIT ?",
            (symbol.upper(), limit),
        ).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d.pop("id", None)
        d.pop("fetched_at", None)
        results.append(d)
    return results


@mcp.tool()
def get_economic_indicators(series_ids: list[str] | None = None) -> list[dict]:
    """Latest values for economic indicators (FRED series).

    Pass series_ids to filter, or None for all.
    """
    df = db.get_latest_economic_indicators(series_ids)
    if df.empty:
        return []
    return df.to_dict(orient="records")


@mcp.tool()
def get_watchlist() -> dict:
    """Current watchlist configuration (categories → symbols)."""
    return WATCHLIST


# ------------------------------------------------------------------
# Analysis tools
# ------------------------------------------------------------------


@mcp.tool()
def analyze_price_technicals(symbol: str, days: int = 90) -> dict:
    """Compute technical indicators for a symbol: SMA(5/10/20), volatility,
    max drawdown, volume trends, and total return over the given period."""
    df = db.get_latest_prices(symbol.upper(), days)
    if df.empty:
        return {"symbol": symbol.upper(), "error": "no price data"}
    df = df.drop(columns=["id", "fetched_at"], errors="ignore")
    return compute_price_technicals(df, symbol.upper())


@mcp.tool()
def screen_valuations(sort_by: str = "pe_ratio") -> list[dict]:
    """Screen all watchlist symbols by valuation metrics (PE, PB, FCF yield,
    earnings yield, etc.). Returns a sorted list — useful for finding
    undervalued or overvalued stocks."""
    df = db.get_all_fundamentals()
    return compute_valuation_screen(df, sort_by)


@mcp.tool()
def get_portfolio_risk_summary() -> dict:
    """Portfolio-level risk analysis: per-symbol volatility and Sharpe ratio,
    average pairwise correlation, sector concentration, and the 3 most/least
    volatile holdings."""
    df = db.get_price_history_batch(ALL_SYMBOLS, days=90)
    return compute_portfolio_risk(df, WATCHLIST)


@mcp.tool()
def get_macro_snapshot() -> dict:
    """Macro environment summary: latest values and 1m/3m/6m/1y changes for
    all tracked FRED indicators, plus yield curve status, VIX regime, and
    Fed Funds rate direction."""
    histories = {}
    for series_id in ECONOMIC_INDICATORS:
        histories[series_id] = db.get_economic_indicator_history(series_id, days=365)
    return compute_macro_snapshot(histories)


@mcp.tool()
def get_sec_activity_summary(days: int = 90) -> dict:
    """SEC filing activity: per-symbol filing counts and types, recent 8-K
    events, and symbols with no filings in the lookback window."""
    # Fetch all filings for watchlist symbols
    import sqlite3 as _sqlite3

    placeholders = ", ".join(["?"] * len(ALL_SYMBOLS))
    query = f"""
        SELECT symbol, filing_date, report_type, primary_doc_description
        FROM sec_filings
        WHERE symbol IN ({placeholders})
        ORDER BY filing_date DESC
    """
    with _sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=ALL_SYMBOLS)
    return compute_sec_activity(df, days)


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
