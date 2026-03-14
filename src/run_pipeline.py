"""Main entry point for running the OpenBB data pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import argparse
from datetime import datetime

from database import Database
from economic_dashboard import EconomicDashboard
from sec_parser import SECParser
from watchlist_fetcher import WatchlistFetcher


def run_full_pipeline():
    """Run the complete data pipeline."""
    print("=" * 70)
    print("OpenBB Financial Data Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    # Initialize database
    print("Initializing database...")
    db = Database()
    print(f"Database location: {db.db_path}")
    print()

    # 1. Watchlist Data Fetcher
    print("=" * 70)
    print("Phase 1: Watchlist Data Fetcher")
    print("=" * 70)
    fetcher = WatchlistFetcher()
    print(f"Symbols to track: {', '.join(fetcher.symbols)}")
    print()

    print("Fetching prices...")
    fetcher.update_all_prices(days=30)
    print()

    print("Fetching fundamentals...")
    fetcher.update_all_fundamentals()
    print()

    print("Fetching SEC filings...")
    fetcher.update_all_sec_filings(limit=10)
    print()

    # Show watchlist summary
    print("Watchlist Summary:")
    print("-" * 70)
    try:
        summary = fetcher.get_watchlist_summary()
        if not summary.empty:
            print(summary.to_string(index=False))
        else:
            print("No data available yet.")
    except Exception as e:
        print(f"Error generating summary: {e}")
    print()

    # 2. SEC Filings Parser
    print("=" * 70)
    print("Phase 2: SEC Filings Parser")
    print("=" * 70)
    parser = SECParser()

    # Generate reports for key symbols
    key_symbols = ["AAPL", "MSFT", "GOOGL", "NVDA", "BABA"]
    for symbol in key_symbols:
        print(f"\n{symbol}:")
        print("-" * 40)
        try:
            report = parser.generate_filing_report(symbol)
            # Print just the first few lines
            for line in report.split("\n")[:15]:
                print(line)
            print("...")
        except Exception as e:
            print(f"Error: {e}")
    print()

    # 3. Economic Indicators Dashboard
    print("=" * 70)
    print("Phase 3: Economic Indicators Dashboard")
    print("=" * 70)
    dashboard = EconomicDashboard()
    dashboard.update_all_indicators()
    print()

    # Generate dashboard report
    try:
        report = dashboard.generate_dashboard_report()
        print(report)
    except Exception as e:
        print(f"Error generating dashboard report: {e}")
    print()

    # Summary
    print("=" * 70)
    print("Pipeline Complete")
    print("=" * 70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Next steps:")
    print("  - Query the SQLite database at: ~/.openbb_platform/data/openbb_data.db")
    print("  - Use watchlist_fetcher.py for daily updates")
    print("  - Use sec_parser.py for filing analysis")
    print("  - Get FRED API key for economic indicators: https://fred.stlouisfed.org/")


def run_quick_test():
    """Run a quick test of the pipeline."""
    print("=" * 70)
    print("OpenBB Pipeline Quick Test")
    print("=" * 70)
    print()

    # Test database
    print("Testing database...")
    db = Database()
    print(f"✓ Database initialized: {db.db_path}")
    print()

    # Test watchlist fetcher
    print("Testing watchlist fetcher...")
    fetcher = WatchlistFetcher()
    print(f"✓ Symbols: {', '.join(fetcher.symbols[:5])}...")

    # Test single price fetch
    print("  Fetching AAPL prices...", end=" ")
    df = fetcher.fetch_prices("AAPL", days=5)
    if df is not None and not df.empty:
        print(f"✓ ({len(df)} rows)")
    else:
        print("✗")

    # Test fundamentals
    print("  Fetching AAPL fundamentals...", end=" ")
    df = fetcher.fetch_fundamentals("AAPL")
    if df is not None and not df.empty:
        print("✓")
    else:
        print("✗")
    print()

    # Test SEC parser
    print("Testing SEC parser...")
    parser = SECParser()
    print("  Fetching AAPL filings...", end=" ")
    filings = parser.fetch_filings("AAPL", limit=5)
    if not filings.empty:
        print(f"✓ ({len(filings)} rows)")
    else:
        print("✗")
    print()

    # Test economic dashboard
    print("Testing economic dashboard...")
    dashboard = EconomicDashboard()
    print("  Fetching GDP real...", end=" ")
    gdp = dashboard.fetch_gdp_real()
    if gdp is not None and not gdp.empty:
        print(f"✓ ({len(gdp)} rows)")
    else:
        print("✗")

    print()
    print("=" * 70)
    print("Quick test complete!")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="OpenBB Financial Data Pipeline")
    parser.add_argument(
        "mode",
        choices=["full", "test", "prices", "fundamentals", "sec", "economic"],
        default="test",
        nargs="?",
        help="Pipeline mode to run",
    )

    args = parser.parse_args()

    if args.mode == "full":
        run_full_pipeline()
    elif args.mode == "test":
        run_quick_test()
    elif args.mode == "prices":
        print("Running prices update...")
        fetcher = WatchlistFetcher()
        fetcher.update_all_prices(days=30)
    elif args.mode == "fundamentals":
        print("Running fundamentals update...")
        fetcher = WatchlistFetcher()
        fetcher.update_all_fundamentals()
    elif args.mode == "sec":
        print("Running SEC filings update...")
        fetcher = WatchlistFetcher()
        fetcher.update_all_sec_filings(limit=10)
    elif args.mode == "economic":
        print("Running economic indicators update...")
        dashboard = EconomicDashboard()
        dashboard.update_all_indicators()


if __name__ == "__main__":
    main()
