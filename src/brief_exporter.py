"""Export OpenBB data as JSON for the portfolio brief generator.

Loads latest data from SQLite, runs analysis functions, and writes a
structured JSON file that the briefs pipeline can consume.

Usage:
    python src/brief_exporter.py [--output PATH]
"""

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent))

from analysis import (
    compute_macro_snapshot,
    compute_portfolio_risk,
    compute_price_technicals,
    compute_sec_activity,
    compute_valuation_screen,
)
from config import DATA_DIR, DB_PATH, ECONOMIC_INDICATORS, WATCHLIST
from database import Database
from report import identify_alerts

# Flat list of all watchlist symbols (deduplicated, sorted)
ALL_SYMBOLS = sorted(set(s for symbols in WATCHLIST.values() for s in symbols))

DEFAULT_OUTPUT = DATA_DIR / "brief_data.json"


def _build_portfolio_snapshot(db: Database) -> list[dict]:
    """Latest prices and daily change % for all watchlist symbols."""
    df = db.get_latest_prices_batch_with_previous(ALL_SYMBOLS)
    if df.empty:
        return []

    symbol_sector = {}
    for category, symbols in WATCHLIST.items():
        for s in symbols:
            if s not in symbol_sector:
                symbol_sector[s] = category

    results = []
    for symbol in df["symbol"].unique():
        rows = df[df["symbol"] == symbol].sort_values("date", ascending=False)
        latest = rows.iloc[0]
        price = latest["close"]
        volume = int(latest.get("volume", 0) or 0)
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
                "price": round(price, 2),
                "change_pct": change_pct,
                "volume": volume,
            }
        )
    return results


def _build_technicals(db: Database) -> dict[str, dict]:
    """Compute technicals for all symbols."""
    technicals = {}
    for sym in ALL_SYMBOLS:
        df = db.get_latest_prices(sym, 90)
        if not df.empty:
            df = df.drop(columns=["id", "fetched_at"], errors="ignore")
            technicals[sym] = compute_price_technicals(df, sym)
    return technicals


def _build_sec_activity(days: int = 90) -> dict:
    """SEC filing activity summary."""
    placeholders = ", ".join(["?"] * len(ALL_SYMBOLS))
    query = f"""
        SELECT symbol, filing_date, report_type, primary_doc_description
        FROM sec_filings
        WHERE symbol IN ({placeholders})
        ORDER BY filing_date DESC
    """
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=ALL_SYMBOLS)
    return compute_sec_activity(df, days)


def export_brief_data(output_path: str | Path | None = None) -> dict:
    """Load OpenBB data, run analyses, and export as JSON.

    Returns the exported data dict. Also writes to output_path.
    """
    db = Database()
    now = datetime.now()

    # 1. Portfolio snapshot
    portfolio_snapshot = _build_portfolio_snapshot(db)

    # 2. Technicals
    technicals = _build_technicals(db)

    # 3. Valuations
    valuations = compute_valuation_screen(db.get_all_fundamentals())

    # 4. Portfolio risk
    prices_df = db.get_price_history_batch(ALL_SYMBOLS, days=90)
    risk_summary = compute_portfolio_risk(prices_df, WATCHLIST)

    # 5. Macro snapshot
    histories = {}
    for series_id in ECONOMIC_INDICATORS:
        histories[series_id] = db.get_economic_indicator_history(series_id, days=365)
    macro_snapshot = compute_macro_snapshot(histories)

    # 6. SEC activity
    sec_activity = _build_sec_activity(days=90)

    # 7. Alerts
    alerts = identify_alerts(
        technicals,
        macro_snapshot,
        sec_activity,
        portfolio_snapshot=portfolio_snapshot,
        risk_summary=risk_summary,
        valuations=valuations,
    )

    data = {
        "generated_at": now.isoformat(),
        "date": now.strftime("%Y-%m-%d"),
        "portfolio_snapshot": portfolio_snapshot,
        "technical_signals": technicals,
        "valuation_check": valuations,
        "risk_dashboard": risk_summary,
        "macro_snapshot": macro_snapshot,
        "sec_activity": sec_activity,
        "alerts": alerts,
    }

    # Write JSON
    out = Path(output_path) if output_path else DEFAULT_OUTPUT
    out.parent.mkdir(parents=True, exist_ok=True)

    # Convert any non-serializable values (NaN, numpy types)
    def _default(obj):
        if isinstance(obj, float) and (obj != obj):  # NaN check
            return None
        if hasattr(obj, "item"):  # numpy scalar
            return obj.item()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    out.write_text(json.dumps(data, indent=2, default=_default), encoding="utf-8")
    print(f"Brief data exported to {out} ({out.stat().st_size:,} bytes)")
    return data


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export OpenBB data for brief generator")
    parser.add_argument("--output", "-o", help="Output JSON path", default=None)
    args = parser.parse_args()
    export_brief_data(args.output)
