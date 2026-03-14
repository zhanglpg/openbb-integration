#!/usr/bin/env python3
"""
Daily Data Pipeline - Fetch and store data for watchlist symbols
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config import PIPELINE_DEFAULTS
from database import Database
from fetcher import DataFetcher
from watchlist import WatchlistManager

logger = logging.getLogger(__name__)


def fetch_daily_prices(watchlist: WatchlistManager, fetcher: DataFetcher, db: Database):
    """Fetch daily prices for all symbols in watchlist"""
    logger.info("=" * 60)
    logger.info("Fetching Daily Prices - %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    logger.info("=" * 60)

    for symbol in watchlist:
        logger.info("[%s] Fetching historical prices...", symbol)
        try:
            df = fetcher.fetch_historical_prices(symbol, provider='yfinance')

            if not df.empty:
                db.save_prices(df, symbol)
                db.log_fetch(symbol, 'prices', 'yfinance', 'success', record_count=len(df))
                logger.info("  Saved %d rows for %s", len(df), symbol)
            else:
                db.log_fetch(symbol, 'prices', 'yfinance', 'error', 'Empty data', 0)
                logger.warning("  No data returned for %s", symbol)

        except Exception as e:
            db.log_fetch(symbol, 'prices', 'yfinance', 'error', str(e), 0)
            logger.error("  Error for %s: %s", symbol, e)


def fetch_fundamentals(watchlist: WatchlistManager, fetcher: DataFetcher, db: Database):
    """Fetch fundamental data for all symbols"""
    logger.info("=" * 60)
    logger.info("Fetching Fundamentals - %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    logger.info("=" * 60)

    data_types = [
        ('income', 'Income Statement'),
        ('metrics', 'Key Metrics'),
    ]

    for symbol in watchlist:
        logger.info("[%s]", symbol)

        for data_type, label in data_types:
            try:
                logger.info("  Fetching %s...", label)

                if data_type == 'income':
                    df = fetcher.fetch_income_statement(symbol, provider='yfinance')
                elif data_type == 'metrics':
                    df = fetcher.fetch_metrics(symbol, provider='yfinance')
                else:
                    continue

                if not df.empty:
                    db.save_fundamentals(df, symbol)
                    db.log_fetch(symbol, data_type, 'yfinance', 'success', record_count=len(df))
                    logger.info("    Saved %d rows", len(df))
                else:
                    db.log_fetch(symbol, data_type, 'yfinance', 'error', 'Empty data', 0)
                    logger.warning("    No data returned")

            except Exception as e:
                db.log_fetch(symbol, data_type, 'yfinance', 'error', str(e), 0)
                logger.error("    Error: %s", e)


def fetch_sec_filings(
    watchlist: WatchlistManager, fetcher: DataFetcher, db: Database, limit: int = None,
):
    """Fetch SEC filings for all symbols"""
    if limit is None:
        limit = PIPELINE_DEFAULTS["sec_filing_limit"]

    logger.info("=" * 60)
    logger.info("Fetching SEC Filings - %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    logger.info("=" * 60)

    for symbol in watchlist:
        logger.info("[%s] Fetching SEC filings...", symbol)
        try:
            df = fetcher.fetch_sec_filings(symbol, provider='sec', limit=limit)

            if not df.empty:
                # Filter for 10-K and 10-Q only
                filing_types = ['10-K', '10-Q', '10-K/A', '10-Q/A']
                filings_of_interest = df[df['report_type'].isin(filing_types)]

                if len(filings_of_interest) > 0:
                    db.save_sec_filings(filings_of_interest, symbol)
                    count = len(filings_of_interest)
                    db.log_fetch(symbol, 'sec_filings', 'sec', 'success', record_count=count)
                    logger.info("  Saved %d filings (10-K/10-Q only)", len(filings_of_interest))

                    # Show latest filings
                    latest = filings_of_interest.head(3)
                    for _, row in latest.iterrows():
                        logger.info("    - %s (%s)", row['report_type'], row['filing_date'])
                else:
                    db.log_fetch(symbol, 'sec_filings', 'sec', 'warning', 'No 10-K/10-Q found', 0)
                    logger.warning("  No 10-K/10-Q filings found")
            else:
                db.log_fetch(symbol, 'sec_filings', 'sec', 'error', 'Empty data', 0)
                logger.warning("  No data returned")

        except Exception as e:
            db.log_fetch(symbol, 'sec_filings', 'sec', 'error', str(e), 0)
            logger.error("  Error: %s", e)


def main():
    """Run daily data pipeline"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("OpenBB Daily Data Pipeline - %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Initialize components
    watchlist = WatchlistManager()
    fetcher = DataFetcher()
    db = Database()

    logger.info("Watchlist: %d symbols", len(watchlist))
    logger.info("Symbols: %s...", ", ".join(watchlist.get_symbols()[:5]))

    # Run pipeline stages
    fetch_daily_prices(watchlist, fetcher, db)
    fetch_fundamentals(watchlist, fetcher, db)
    fetch_sec_filings(watchlist, fetcher, db)

    # Summary
    logger.info("=" * 60)
    logger.info("Pipeline Complete!")
    logger.info("=" * 60)

    history = db.get_fetch_history(limit=20)
    if not history.empty:
        logger.info("Recent fetch history:")
        for _, row in history.head(10).iterrows():
            status_icon = "OK" if row['status'] == 'success' else "FAIL"
            logger.info(
                "  %s %6s | %-15s | %4d records",
                status_icon, row['symbol'], row['data_type'], row['record_count'],
            )


if __name__ == "__main__":
    main()
