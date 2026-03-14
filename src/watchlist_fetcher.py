"""Watchlist data fetcher - fetches prices and fundamentals for tracked symbols."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
from openbb import obb

from config import WATCHLIST, REFRESH_INTERVALS
from database import Database


class WatchlistFetcher:
    """Fetches and stores watchlist data from OpenBB."""
    
    def __init__(self):
        self.db = Database()
        self.symbols = self._flatten_watchlist()
        obb.user.preferences.output_type = "dataframe"
    
    def _flatten_watchlist(self) -> List[str]:
        """Flatten watchlist categories into unique symbols."""
        symbols = set()
        for category, syms in WATCHLIST.items():
            symbols.update(syms)
        return sorted(list(symbols))
    
    def fetch_prices(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """Fetch historical prices for a symbol."""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            data = obb.equity.price.historical(
                symbol=symbol,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['symbol'] = symbol
                df['fetched_at'] = datetime.now().isoformat()
                return df
        except Exception as e:
            print(f"Error fetching prices for {symbol}: {e}")
        return None
    
    def fetch_fundamentals(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch fundamental metrics for a symbol."""
        try:
            data = obb.equity.fundamental.metrics(symbol=symbol)
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df['symbol'] = symbol
                df['fetched_at'] = datetime.now().isoformat()
                return df
        except Exception as e:
            print(f"Error fetching fundamentals for {symbol}: {e}")
        return None
    
    def fetch_sec_filings(self, symbol: str, limit: int = 50) -> Optional[pd.DataFrame]:
        """Fetch SEC filings for a symbol."""
        try:
            data = obb.equity.fundamental.filings(symbol=symbol, provider="sec")
            
            if data is not None:
                df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
                df = df.head(limit)
                df['symbol'] = symbol
                df['fetched_at'] = datetime.now().isoformat()
                return df
        except Exception as e:
            print(f"Error fetching SEC filings for {symbol}: {e}")
        return None
    
    def update_all_prices(self, days: int = 30):
        """Update prices for all watchlist symbols."""
        print(f"Updating prices for {len(self.symbols)} symbols...")
        
        for symbol in self.symbols:
            print(f"  Fetching {symbol}...", end=" ")
            df = self.fetch_prices(symbol, days)
            
            if df is not None and not df.empty:
                self.db.save_prices(df, symbol)
                self.db.update_metadata("prices", symbol, len(df), "success")
                print(f"✓ ({len(df)} rows)")
            else:
                self.db.update_metadata("prices", symbol, 0, "failed")
                print("✗")
    
    def update_all_fundamentals(self):
        """Update fundamentals for all watchlist symbols."""
        print(f"Updating fundamentals for {len(self.symbols)} symbols...")
        
        for symbol in self.symbols:
            print(f"  Fetching {symbol}...", end=" ")
            df = self.fetch_fundamentals(symbol)
            
            if df is not None and not df.empty:
                self.db.save_fundamentals(df, symbol)
                self.db.update_metadata("fundamentals", symbol, len(df), "success")
                print(f"✓")
            else:
                self.db.update_metadata("fundamentals", symbol, 0, "failed")
                print("✗")
    
    def update_all_sec_filings(self, limit: int = 20):
        """Update SEC filings for all watchlist symbols."""
        print(f"Updating SEC filings for {len(self.symbols)} symbols...")
        
        for symbol in self.symbols:
            print(f"  Fetching {symbol}...", end=" ")
            df = self.fetch_sec_filings(symbol, limit)
            
            if df is not None and not df.empty:
                self.db.save_sec_filings(df, symbol)
                self.db.update_metadata("sec_filings", symbol, len(df), "success")
                print(f"✓ ({len(df)} rows)")
            else:
                self.db.update_metadata("sec_filings", symbol, 0, "failed")
                print("✗")
    
    def get_watchlist_summary(self) -> pd.DataFrame:
        """Get summary of all watchlist symbols with latest prices."""
        summaries = []
        
        for symbol in self.symbols:
            prices = self.db.get_latest_prices(symbol, days=2)
            if not prices.empty and len(prices) >= 2:
                latest = prices.iloc[0]
                previous = prices.iloc[1]
                change = latest['close'] - previous['close']
                change_pct = (change / previous['close']) * 100
                
                summaries.append({
                    'symbol': symbol,
                    'latest_close': latest['close'],
                    'previous_close': previous['close'],
                    'change': change,
                    'change_pct': change_pct,
                    'volume': latest['volume'],
                    'date': latest['date']
                })
        
        return pd.DataFrame(summaries)


if __name__ == "__main__":
    fetcher = WatchlistFetcher()
    
    print("=" * 60)
    print("OpenBB Watchlist Fetcher")
    print("=" * 60)
    print(f"\nSymbols to track: {', '.join(fetcher.symbols)}")
    print()
    
    # Update all data
    fetcher.update_all_prices(days=30)
    print()
    fetcher.update_all_fundamentals()
    print()
    fetcher.update_all_sec_filings(limit=10)
    print()
    
    # Show summary
    print("=" * 60)
    print("Watchlist Summary")
    print("=" * 60)
    summary = fetcher.get_watchlist_summary()
    print(summary.to_string(index=False))
