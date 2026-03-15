"""Main entry point for running the OpenBB data pipeline."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import argparse
import sqlite3
from datetime import datetime

import pandas as pd

from analysis import (
    compute_macro_snapshot,
    compute_portfolio_risk,
    compute_price_technicals,
    compute_sec_activity,
    compute_valuation_screen,
)
from config import DB_PATH, ECONOMIC_INDICATORS, REPORTS_DIR, WATCHLIST
from database import Database
from economic_dashboard import EconomicDashboard
from report import format_report_markdown, generate_daily_report
from sec_parser import SECParser
from watchlist_fetcher import WatchlistFetcher

logger = logging.getLogger(__name__)


def run_full_pipeline():
    """Run the complete data pipeline."""
    logger.info("=" * 70)
    logger.info("OpenBB Financial Data Pipeline")
    logger.info("Started: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 70)

    # Initialize database
    logger.info("Initializing database...")
    db = Database()
    logger.info("Database location: %s", db.db_path)

    # 1. Watchlist Data Fetcher
    logger.info("=" * 70)
    logger.info("Phase 1: Watchlist Data Fetcher")
    logger.info("=" * 70)
    fetcher = WatchlistFetcher()
    logger.info("Symbols to track: %s", ", ".join(fetcher.symbols))

    fetcher.update_all_prices()
    fetcher.update_all_fundamentals()
    fetcher.update_all_sec_filings()

    # Show watchlist summary
    logger.info("Watchlist Summary:")
    try:
        summary = fetcher.get_watchlist_summary()
        if not summary.empty:
            logger.info("\n%s", summary.to_string(index=False))
        else:
            logger.info("No data available yet.")
    except Exception as e:
        logger.error("Error generating summary: %s", e)

    # 2. SEC Filings Parser
    logger.info("=" * 70)
    logger.info("Phase 2: SEC Filings Parser")
    logger.info("=" * 70)
    parser = SECParser()

    # Generate reports for key symbols
    key_symbols = ["AAPL", "MSFT", "GOOGL", "NVDA", "BABA"]
    for symbol in key_symbols:
        logger.info("%s:", symbol)
        try:
            report = parser.generate_filing_report(symbol)
            # Print just the first few lines
            for line in report.split("\n")[:15]:
                logger.info(line)
            logger.info("...")
        except Exception as e:
            logger.error("Error: %s", e)

    # 3. Economic Indicators Dashboard
    logger.info("=" * 70)
    logger.info("Phase 3: Economic Indicators Dashboard")
    logger.info("=" * 70)
    dashboard = EconomicDashboard()
    dashboard.update_all_indicators()

    # Generate dashboard report
    try:
        report = dashboard.generate_dashboard_report()
        logger.info("\n%s", report)
    except Exception as e:
        logger.error("Error generating dashboard report: %s", e)

    # Summary
    logger.info("=" * 70)
    logger.info("Pipeline Complete")
    logger.info("=" * 70)
    logger.info("Finished: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Next steps:")
    logger.info("  - Query the SQLite database at: data/openbb_data.db")
    logger.info("  - Use watchlist_fetcher.py for daily updates")
    logger.info("  - Use sec_parser.py for filing analysis")
    logger.info("  - Get FRED API key for economic indicators: https://fred.stlouisfed.org/")


def run_quick_test():
    """Run a quick test of the pipeline."""
    logger.info("=" * 70)
    logger.info("OpenBB Pipeline Quick Test")
    logger.info("=" * 70)

    # Test database
    logger.info("Testing database...")
    db = Database()
    logger.info("Database initialized: %s", db.db_path)

    # Test watchlist fetcher
    logger.info("Testing watchlist fetcher...")
    fetcher = WatchlistFetcher()
    logger.info("Symbols: %s...", ", ".join(fetcher.symbols[:5]))

    # Test single price fetch
    logger.info("  Fetching AAPL prices...")
    df = fetcher.fetch_prices("AAPL", days=5)
    if df is not None and not df.empty:
        logger.info("  AAPL prices: %d rows", len(df))
    else:
        logger.warning("  AAPL prices: no data")

    # Test fundamentals
    logger.info("  Fetching AAPL fundamentals...")
    df = fetcher.fetch_fundamentals("AAPL")
    if df is not None and not df.empty:
        logger.info("  AAPL fundamentals: OK")
    else:
        logger.warning("  AAPL fundamentals: no data")

    # Test SEC parser
    logger.info("Testing SEC parser...")
    parser = SECParser()
    logger.info("  Fetching AAPL filings...")
    filings = parser.fetch_filings("AAPL", limit=5)
    if not filings.empty:
        logger.info("  AAPL filings: %d rows", len(filings))
    else:
        logger.warning("  AAPL filings: no data")

    # Test economic dashboard
    logger.info("Testing economic dashboard...")
    dashboard = EconomicDashboard()
    logger.info("  Fetching GDP real...")
    gdp = dashboard.fetch_gdp_real()
    if gdp is not None and not gdp.empty:
        logger.info("  GDP real: %d rows", len(gdp))
    else:
        logger.warning("  GDP real: no data")

    logger.info("=" * 70)
    logger.info("Quick test complete!")
    logger.info("=" * 70)


def run_daily_report():
    """Generate a daily report from existing DB data."""
    logger.info("=" * 70)
    logger.info("Daily Report Generation")
    logger.info("=" * 70)

    db = Database()
    all_symbols = sorted(set(s for symbols in WATCHLIST.values() for s in symbols))

    # 1. Portfolio overview
    overview_df = db.get_latest_prices_batch_with_previous(all_symbols)
    symbol_sector = {}
    for category, symbols in WATCHLIST.items():
        for s in symbols:
            if s not in symbol_sector:
                symbol_sector[s] = category

    portfolio_overview = []
    if not overview_df.empty:
        for symbol in overview_df["symbol"].unique():
            rows = overview_df[overview_df["symbol"] == symbol].sort_values("date", ascending=False)
            latest = rows.iloc[0]
            price = latest["close"]
            change_pct = None
            if len(rows) >= 2:
                prev_close = rows.iloc[1]["close"]
                if prev_close and prev_close != 0:
                    change_pct = round((price - prev_close) / prev_close * 100, 2)
            portfolio_overview.append(
                {
                    "symbol": symbol,
                    "sector": symbol_sector.get(symbol, "unknown"),
                    "price": price,
                    "change_pct": change_pct,
                }
            )

    # 2. Technicals
    technicals = {}
    for sym in all_symbols:
        df = db.get_latest_prices(sym, 90)
        if not df.empty:
            df = df.drop(columns=["id", "fetched_at"], errors="ignore")
            technicals[sym] = compute_price_technicals(df, sym)

    # 3. Valuations
    fund_df = db.get_all_fundamentals()
    valuations = compute_valuation_screen(fund_df)

    # 4. Risk
    prices_df = db.get_price_history_batch(all_symbols, days=90)
    risk_summary = compute_portfolio_risk(prices_df, WATCHLIST)

    # 5. Macro
    histories = {}
    for series_id in ECONOMIC_INDICATORS:
        histories[series_id] = db.get_economic_indicator_history(series_id, days=365)
    macro_snapshot = compute_macro_snapshot(histories)

    # 6. SEC activity
    placeholders = ", ".join(["?"] * len(all_symbols))
    query = f"""
        SELECT symbol, filing_date, report_type, primary_doc_description
        FROM sec_filings
        WHERE symbol IN ({placeholders})
        ORDER BY filing_date DESC
    """
    with sqlite3.connect(DB_PATH) as conn:
        filings_df = pd.read_sql_query(query, conn, params=all_symbols)
    sec_activity = compute_sec_activity(filings_df, days=90)

    # Generate report
    report_date = datetime.now().strftime("%Y-%m-%d")
    report = generate_daily_report(
        portfolio_overview=portfolio_overview,
        technicals=technicals,
        valuations=valuations,
        risk_summary=risk_summary,
        macro_snapshot=macro_snapshot,
        sec_activity=sec_activity,
        report_date=report_date,
    )
    md = format_report_markdown(report)

    # Write to file
    report_path = REPORTS_DIR / f"{report_date}.md"
    report_path.write_text(md, encoding="utf-8")
    logger.info("Report written to: %s", report_path)
    logger.info("Report date: %s", report_date)


def main():
    parser = argparse.ArgumentParser(description="OpenBB Financial Data Pipeline")
    parser.add_argument(
        "mode",
        choices=["full", "test", "prices", "fundamentals", "sec", "economic", "report", "daily"],
        default="test",
        nargs="?",
        help="Pipeline mode to run",
    )

    args = parser.parse_args()

    # Configure logging for CLI usage
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.mode == "full":
        run_full_pipeline()
    elif args.mode == "test":
        run_quick_test()
    elif args.mode == "prices":
        logger.info("Running prices update...")
        fetcher = WatchlistFetcher()
        fetcher.update_all_prices()
    elif args.mode == "fundamentals":
        logger.info("Running fundamentals update...")
        fetcher = WatchlistFetcher()
        fetcher.update_all_fundamentals()
    elif args.mode == "sec":
        logger.info("Running SEC filings update...")
        fetcher = WatchlistFetcher()
        fetcher.update_all_sec_filings()
    elif args.mode == "economic":
        logger.info("Running economic indicators update...")
        dashboard = EconomicDashboard()
        dashboard.update_all_indicators()
    elif args.mode == "report":
        run_daily_report()
    elif args.mode == "daily":
        run_full_pipeline()
        run_daily_report()


if __name__ == "__main__":
    main()
