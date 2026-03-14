#!/usr/bin/env python3
"""
Quick Query Script - Test data access and display
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import pandas as pd

from database import Database
from watchlist import WatchlistManager

logger = logging.getLogger(__name__)


def show_prices(symbol: str, db: Database, limit: int = 5):
    """Show latest prices"""
    df = db.get_latest_prices(symbol, days=limit)
    if df.empty:
        logger.info("No price data for %s", symbol)
        return

    logger.info("=" * 60)
    logger.info("%s - Latest Prices (Last %d days)", symbol, limit)
    logger.info("=" * 60)

    # Show date and key columns
    cols_to_show = ['close', 'open', 'high', 'low', 'volume']
    available_cols = [c for c in cols_to_show if c in df.columns]

    if 'date' in df.columns:
        df_sorted = df.sort_values('date', ascending=False)
        print(df_sorted[['date'] + available_cols].head(limit).to_string(index=False))
    else:
        print(df[available_cols].tail(limit).to_string(index=False))


def show_metrics(symbol: str, db: Database):
    """Show key metrics from fundamentals table"""
    import sqlite3
    query = "SELECT * FROM fundamentals WHERE symbol = ? ORDER BY fetched_at DESC LIMIT 1"
    with sqlite3.connect(db.db_path) as conn:
        df = pd.read_sql_query(query, conn, params=(symbol,))

    if df.empty:
        logger.info("No metrics data for %s", symbol)
        return

    logger.info("=" * 60)
    logger.info("%s - Key Metrics", symbol)
    logger.info("=" * 60)

    key_cols = ['market_cap', 'pe_ratio', 'pb_ratio',
                'debt_to_equity', 'return_on_equity', 'dividend_yield']

    available_cols = [c for c in key_cols if c in df.columns]

    if not available_cols:
        available_cols = df.columns[:8].tolist()

    for col in available_cols:
        if col in df.columns:
            val = df[col].iloc[0]
            if pd.notna(val):
                if col == 'market_cap':
                    print(f"  {col:25}: ${val/1e9:.2f}B")
                elif col in ['dividend_yield']:
                    print(f"  {col:25}: {val*100:.2f}%")
                elif col in ['pe_ratio', 'pb_ratio']:
                    print(f"  {col:25}: {val:.2f}")
                elif col in ['return_on_equity']:
                    print(f"  {col:25}: {val*100:.2f}%")
                else:
                    print(f"  {col:25}: {val:.2f}")


def show_sec_filings(symbol: str, db: Database, limit: int = 5):
    """Show latest SEC filings"""
    import sqlite3
    query = "SELECT * FROM sec_filings WHERE symbol = ? ORDER BY filing_date DESC LIMIT ?"
    with sqlite3.connect(db.db_path) as conn:
        df = pd.read_sql_query(query, conn, params=(symbol, limit))

    if df.empty:
        logger.info("No SEC filings for %s", symbol)
        return

    # Filter for 10-K and 10-Q
    filings = df[df['report_type'].isin(['10-K', '10-Q', '10-K/A', '10-Q/A'])]

    if filings.empty:
        logger.info("No 10-K/10-Q filings for %s", symbol)
        return

    logger.info("=" * 60)
    logger.info("%s - Latest SEC Filings (10-K/10-Q)", symbol)
    logger.info("=" * 60)

    for _, row in filings.head(limit).iterrows():
        url = row.get('report_url', 'N/A') or 'N/A'
        print(f"  {row['report_type']:8} | {row['filing_date']} | {url[:60]}...")


def main():
    """Run queries"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("OpenBB Data Query")

    db = Database()
    watchlist = WatchlistManager()

    # Show data for first 3 symbols
    for symbol in watchlist.get_symbols()[:3]:
        show_prices(symbol, db)
        show_metrics(symbol, db)
        show_sec_filings(symbol, db)

    logger.info("=" * 60)
    logger.info("Query Complete!")
    logger.info("=" * 60)
    logger.info("Total symbols in watchlist: %d", len(watchlist))
    logger.info("Data directory: %s", db.db_path.parent)


if __name__ == "__main__":
    main()
