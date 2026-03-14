#!/usr/bin/env python3
"""
Quick Query Script - Test data access and display
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import pandas as pd
from watchlist import WatchlistManager
from storage import DataStorage


def show_prices(symbol: str, storage: DataStorage, limit: int = 5):
    """Show latest prices"""
    df = storage.load_prices(symbol)
    if df.empty:
        print(f"No price data for {symbol}")
        return
    
    print(f"\n{'='*60}")
    print(f"{symbol} - Latest Prices (Last {limit} days)")
    print(f"{'='*60}")
    
    # Show date and key columns
    cols_to_show = ['close', 'open', 'high', 'low', 'volume']
    available_cols = [c for c in cols_to_show if c in df.columns]
    
    if 'date' in df.columns:
        df_sorted = df.sort_values('date', ascending=False)
        print(df_sorted[['date'] + available_cols].head(limit).to_string(index=False))
    else:
        print(df[available_cols].tail(limit).to_string(index=False))


def show_metrics(symbol: str, storage: DataStorage):
    """Show key metrics"""
    df = storage.load_fundamentals(symbol, 'metrics')
    if df.empty:
        print(f"No metrics data for {symbol}")
        return
    
    print(f"\n{'='*60}")
    print(f"{symbol} - Key Metrics")
    print(f"{'='*60}")
    
    # Show key metrics
    key_cols = ['market_cap', 'pe_ratio', 'forward_pe', 'profit_margin', 
                'return_on_equity', 'debt_to_equity', 'dividend_yield', 'beta']
    
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
                elif col in ['pe_ratio', 'forward_pe', 'beta']:
                    print(f"  {col:25}: {val:.2f}")
                elif col in ['profit_margin', 'return_on_equity']:
                    print(f"  {col:25}: {val*100:.2f}%")
                else:
                    print(f"  {col:25}: {val:.2f}")


def show_sec_filings(symbol: str, storage: DataStorage, limit: int = 5):
    """Show latest SEC filings"""
    import glob
    
    pattern = str(storage.sec_dir / f"{symbol}_sec_filings_*.parquet")
    files = sorted(glob.glob(pattern))
    
    if not files:
        print(f"No SEC filings for {symbol}")
        return
    
    df = pd.read_parquet(files[-1])
    
    # Filter for 10-K and 10-Q
    filings = df[df['report_type'].isin(['10-K', '10-Q', '10-K/A', '10-Q/A'])]
    
    if filings.empty:
        print(f"No 10-K/10-Q filings for {symbol}")
        return
    
    print(f"\n{'='*60}")
    print(f"{symbol} - Latest SEC Filings (10-K/10-Q)")
    print(f"{'='*60}")
    
    for _, row in filings.head(limit).iterrows():
        print(f"  {row['report_type']:8} | {row['filing_date']} | {row['report_url'][:60]}...")


def main():
    """Run queries"""
    print(f"""
╔═══════════════════════════════════════════════════════════╗
║           OpenBB Data Query                                ║
╚═══════════════════════════════════════════════════════════╝
    """)
    
    storage = DataStorage()
    watchlist = WatchlistManager()
    
    # Show data for first 3 symbols
    for symbol in watchlist.get_symbols()[:3]:
        show_prices(symbol, storage)
        show_metrics(symbol, storage)
        show_sec_filings(symbol, storage)
    
    print(f"\n{'='*60}")
    print("Query Complete!")
    print(f"{'='*60}")
    print(f"\nTotal symbols in watchlist: {len(watchlist)}")
    print(f"Data directory: {storage.base_path}")


if __name__ == "__main__":
    main()
