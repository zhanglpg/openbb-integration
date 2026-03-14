#!/usr/bin/env python3
"""
Daily Data Pipeline - Fetch and store data for watchlist symbols
"""
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from watchlist import WatchlistManager
from fetcher import DataFetcher
from storage import DataStorage


def fetch_daily_prices(watchlist: WatchlistManager, fetcher: DataFetcher, storage: DataStorage):
    """Fetch daily prices for all symbols in watchlist"""
    print(f"\n{'='*60}")
    print(f"Fetching Daily Prices - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    
    for symbol in watchlist:
        print(f"\n[{symbol}] Fetching historical prices...")
        try:
            df = fetcher.fetch_historical_prices(symbol, provider='yfinance')
            
            if not df.empty:
                filepath = storage.save_prices(symbol, df)
                storage.log_fetch(symbol, 'prices', 'yfinance', 'success', record_count=len(df))
                print(f"  ✓ Saved {len(df)} rows to {filepath}")
            else:
                storage.log_fetch(symbol, 'prices', 'yfinance', 'error', 'Empty data', 0)
                print(f"  ✗ No data returned")
                
        except Exception as e:
            storage.log_fetch(symbol, 'prices', 'yfinance', 'error', str(e), 0)
            print(f"  ✗ Error: {e}")


def fetch_fundamentals(watchlist: WatchlistManager, fetcher: DataFetcher, storage: DataStorage):
    """Fetch fundamental data for all symbols"""
    print(f"\n{'='*60}")
    print(f"Fetching Fundamentals - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    
    data_types = [
        ('income', 'Income Statement'),
        ('metrics', 'Key Metrics'),
    ]
    
    for symbol in watchlist:
        print(f"\n[{symbol}]")
        
        for data_type, label in data_types:
            try:
                print(f"  Fetching {label}...")
                
                if data_type == 'income':
                    df = fetcher.fetch_income_statement(symbol, provider='yfinance')
                elif data_type == 'metrics':
                    df = fetcher.fetch_metrics(symbol, provider='yfinance')
                else:
                    continue
                
                if not df.empty:
                    filepath = storage.save_fundamentals(symbol, data_type, df)
                    storage.log_fetch(symbol, data_type, 'yfinance', 'success', record_count=len(df))
                    print(f"    ✓ Saved {len(df)} rows")
                else:
                    storage.log_fetch(symbol, data_type, 'yfinance', 'error', 'Empty data', 0)
                    print(f"    ✗ No data returned")
                    
            except Exception as e:
                storage.log_fetch(symbol, data_type, 'yfinance', 'error', str(e), 0)
                print(f"    ✗ Error: {e}")


def fetch_sec_filings(watchlist: WatchlistManager, fetcher: DataFetcher, storage: DataStorage, limit: int = 50):
    """Fetch SEC filings for all symbols"""
    print(f"\n{'='*60}")
    print(f"Fetching SEC Filings - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    
    for symbol in watchlist:
        print(f"\n[{symbol}] Fetching SEC filings...")
        try:
            df = fetcher.fetch_sec_filings(symbol, provider='sec', limit=limit)
            
            if not df.empty:
                # Filter for 10-K and 10-Q only
                filings_of_interest = df[df['report_type'].isin(['10-K', '10-Q', '10-K/A', '10-Q/A'])]
                
                if len(filings_of_interest) > 0:
                    filepath = storage.save_sec_filings(symbol, filings_of_interest)
                    storage.log_fetch(symbol, 'sec_filings', 'sec', 'success', record_count=len(filings_of_interest))
                    print(f"  ✓ Saved {len(filings_of_interest)} filings (10-K/10-Q only)")
                    
                    # Show latest filings
                    latest = filings_of_interest.head(3)
                    for _, row in latest.iterrows():
                        print(f"    - {row['report_type']} ({row['filing_date']})")
                else:
                    storage.log_fetch(symbol, 'sec_filings', 'sec', 'error', 'No 10-K/10-Q found', 0)
                    print(f"  ✗ No 10-K/10-Q filings found")
            else:
                storage.log_fetch(symbol, 'sec_filings', 'sec', 'error', 'Empty data', 0)
                print(f"  ✗ No data returned")
                
        except Exception as e:
            storage.log_fetch(symbol, 'sec_filings', 'sec', 'error', str(e), 0)
            print(f"  ✗ Error: {e}")


def main():
    """Run daily data pipeline"""
    print(f"""
╔═══════════════════════════════════════════════════════════╗
║           OpenBB Daily Data Pipeline                       ║
║           {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                          ║
╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Initialize components
    watchlist = WatchlistManager()
    fetcher = DataFetcher()
    storage = DataStorage()
    
    print(f"Watchlist: {len(watchlist)} symbols")
    print(f"Symbols: {', '.join(watchlist.get_symbols()[:5])}...")
    
    # Run pipeline stages
    fetch_daily_prices(watchlist, fetcher, storage)
    fetch_fundamentals(watchlist, fetcher, storage)
    fetch_sec_filings(watchlist, fetcher, storage, limit=100)
    
    # Summary
    print(f"\n{'='*60}")
    print("Pipeline Complete!")
    print(f"{'='*60}")
    
    history = storage.get_fetch_history(limit=20)
    if not history.empty:
        print("\nRecent fetch history:")
        for _, row in history.head(10).iterrows():
            status_icon = "✓" if row['status'] == 'success' else "✗"
            print(f"  {status_icon} {row['symbol']:6} | {row['data_type']:15} | {row['record_count']:4} records")


if __name__ == "__main__":
    main()
