"""SEC filings parser - extracts and analyzes 10-K, 10-Q, 8-K filings."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import pandas as pd
from openbb import obb

from database import Database


class SECParser:
    """Parser for SEC EDGAR filings."""
    
    def __init__(self):
        self.db = Database()
        obb.user.preferences.output_type = "dataframe"
    
    def fetch_filings(self, symbol: str, filing_types: Optional[List[str]] = None, 
                     limit: int = 100) -> pd.DataFrame:
        """
        Fetch SEC filings for a symbol.
        
        Args:
            symbol: Stock ticker symbol
            filing_types: List of filing types to filter (e.g., ['10-K', '10-Q'])
            limit: Maximum number of filings to retrieve
        
        Returns:
            DataFrame with filing information
        """
        try:
            data = obb.equity.fundamental.filings(symbol=symbol, provider="sec")
            
            if data is None:
                return pd.DataFrame()
            
            df = data.to_dataframe() if hasattr(data, 'to_dataframe') else data
            
            # Filter by filing types if specified
            if filing_types:
                df = df[df['report_type'].isin(filing_types)]
            
            return df.head(limit)
        
        except Exception as e:
            print(f"Error fetching SEC filings for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_latest_10k(self, symbol: str) -> Optional[Dict]:
        """Get the most recent 10-K filing for a symbol."""
        filings = self.fetch_filings(symbol, filing_types=['10-K'], limit=1)
        
        if filings.empty:
            return None
        
        filing = filings.iloc[0]
        return {
            'symbol': symbol,
            'filing_date': filing['filing_date'],
            'report_date': filing['report_date'],
            'report_type': filing['report_type'],
            'report_url': filing['report_url'],
            'filing_detail_url': filing['filing_detail_url'],
            'accession_number': filing['accession_number'],
            'primary_doc': filing['primary_doc'],
        }
    
    def get_latest_10q(self, symbol: str) -> Optional[Dict]:
        """Get the most recent 10-Q filing for a symbol."""
        filings = self.fetch_filings(symbol, filing_types=['10-Q'], limit=1)
        
        if filings.empty:
            return None
        
        filing = filings.iloc[0]
        return {
            'symbol': symbol,
            'filing_date': filing['filing_date'],
            'report_date': filing['report_date'],
            'report_type': filing['report_type'],
            'report_url': filing['report_url'],
            'filing_detail_url': filing['filing_detail_url'],
            'accession_number': filing['accession_number'],
            'primary_doc': filing['primary_doc'],
        }
    
    def get_recent_8k(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Get recent 8-K filings (current reports) for a symbol."""
        filings = self.fetch_filings(symbol, filing_types=['8-K'], limit=50)
        
        if filings.empty:
            return pd.DataFrame()
        
        # Filter by date
        filings['filing_date'] = pd.to_datetime(filings['filing_date'])
        cutoff_date = datetime.now() - pd.Timedelta(days=days)
        recent = filings[filings['filing_date'] >= cutoff_date]
        
        return recent
    
    def analyze_filing_frequency(self, symbol: str) -> Dict:
        """Analyze filing frequency and patterns for a symbol."""
        filings = self.fetch_filings(symbol, limit=200)
        
        if filings.empty:
            return {'symbol': symbol, 'error': 'No filings found'}
        
        # Count by filing type
        type_counts = filings['report_type'].value_counts().to_dict()
        
        # Get date range
        filings['filing_date'] = pd.to_datetime(filings['filing_date'])
        date_range = {
            'earliest': filings['filing_date'].min().strftime('%Y-%m-%d'),
            'latest': filings['filing_date'].max().strftime('%Y-%m-%d'),
        }
        
        # Recent 10-K and 10-Q
        latest_10k = self.get_latest_10k(symbol)
        latest_10q = self.get_latest_10q(symbol)
        
        return {
            'symbol': symbol,
            'total_filings': len(filings),
            'filing_types': type_counts,
            'date_range': date_range,
            'latest_10k': latest_10k['filing_date'] if latest_10k else None,
            'latest_10q': latest_10q['filing_date'] if latest_10q else None,
        }
    
    def compare_filings(self, symbols: List[str]) -> pd.DataFrame:
        """Compare filing patterns across multiple symbols."""
        results = []
        
        for symbol in symbols:
            analysis = self.analyze_filing_frequency(symbol)
            results.append({
                'symbol': symbol,
                'total_filings': analysis.get('total_filings', 0),
                'latest_10k': analysis.get('latest_10k'),
                'latest_10q': analysis.get('latest_10q'),
                'filing_types': len(analysis.get('filing_types', {})),
            })
        
        return pd.DataFrame(results)
    
    def generate_filing_report(self, symbol: str) -> str:
        """Generate a text report of recent filings for a symbol."""
        analysis = self.analyze_filing_frequency(symbol)
        
        report = []
        report.append(f"# SEC Filings Report: {symbol}")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        report.append(f"## Overview")
        report.append(f"- Total filings tracked: {analysis.get('total_filings', 0)}")
        report.append(f"- Date range: {analysis.get('date_range', {}).get('earliest')} to {analysis.get('date_range', {}).get('latest')}")
        report.append("")
        
        report.append(f"## Filing Types")
        for ftype, count in analysis.get('filing_types', {}).items():
            report.append(f"- {ftype}: {count}")
        report.append("")
        
        # Latest 10-K
        latest_10k = self.get_latest_10k(symbol)
        if latest_10k:
            report.append(f"## Latest 10-K (Annual Report)")
            report.append(f"- Filing date: {latest_10k['filing_date']}")
            report.append(f"- Report date: {latest_10k['report_date']}")
            report.append(f"- URL: {latest_10k['filing_detail_url']}")
            report.append("")
        
        # Latest 10-Q
        latest_10q = self.get_latest_10q(symbol)
        if latest_10q:
            report.append(f"## Latest 10-Q (Quarterly Report)")
            report.append(f"- Filing date: {latest_10q['filing_date']}")
            report.append(f"- Report date: {latest_10q['report_date']}")
            report.append(f"- URL: {latest_10q['filing_detail_url']}")
            report.append("")
        
        # Recent 8-Ks
        recent_8k = self.get_recent_8k(symbol, days=90)
        if not recent_8k.empty:
            report.append(f"## Recent 8-K Filings (Last 90 Days)")
            for _, row in recent_8k.head(5).iterrows():
                report.append(f"- {row['filing_date'].strftime('%Y-%m-%d')}: {row['primary_doc_description']}")
            report.append("")
        
        return "\n".join(report)


if __name__ == "__main__":
    parser = SECParser()
    
    print("=" * 60)
    print("SEC Filings Parser")
    print("=" * 60)
    print()
    
    # Test with AAPL
    symbol = "AAPL"
    print(f"\nAnalyzing {symbol}...")
    print("-" * 40)
    
    report = parser.generate_filing_report(symbol)
    print(report)
    
    # Compare multiple symbols
    print("\n" + "=" * 60)
    print("Comparing Filing Patterns")
    print("=" * 60)
    symbols = ["AAPL", "MSFT", "GOOGL"]
    comparison = parser.compare_filings(symbols)
    print(comparison.to_string(index=False))
