"""
Storage Layer - SQLite metadata + Parquet time-series data
"""
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, List


class DataStorage:
    """Store financial data in SQLite (metadata) + Parquet (time-series)"""
    
    def __init__(self, base_path: str = None):
        """
        Initialize storage
        
        Args:
            base_path: Base directory for data storage
        """
        if base_path is None:
            base_path = Path(__file__).parent.parent / "data"
        self.base_path = Path(base_path)
        self.db_path = self.base_path / "metadata.db"
        
        # Create directories
        self.prices_dir = self.base_path / "prices"
        self.fundamentals_dir = self.base_path / "fundamentals"
        self.sec_dir = self.base_path / "sec"
        
        for dir_path in [self.prices_dir, self.fundamentals_dir, self.sec_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database with schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Watchlist table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT UNIQUE NOT NULL,
                    name TEXT,
                    sector TEXT,
                    industry TEXT,
                    added_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            """)

            # Data fetch log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fetch_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    provider TEXT,
                    fetch_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    error_message TEXT,
                    record_count INTEGER
                )
            """)

            # SEC filings metadata
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sec_filings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    filing_date TEXT,
                    report_type TEXT,
                    accession_number TEXT UNIQUE,
                    report_url TEXT,
                    filing_url TEXT,
                    stored_path TEXT,
                    fetched_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
    
    def save_prices(self, symbol: str, df: pd.DataFrame) -> str:
        """
        Save price data to parquet
        
        Args:
            symbol: Stock symbol
            df: DataFrame with price data
            
        Returns:
            Path to saved file
        """
        if df.empty:
            return ""
        
        filename = f"{symbol}_prices_{datetime.now().strftime('%Y%m%d')}.parquet"
        filepath = self.prices_dir / filename
        
        # Reset index to include date as column
        df_to_save = df.reset_index() if 'date' not in df.columns else df.copy()
        
        # Add metadata columns
        df_to_save['_symbol'] = symbol
        df_to_save['_fetched_at'] = datetime.now().isoformat()
        
        df_to_save.to_parquet(filepath, index=False)
        return str(filepath)
    
    def load_prices(self, symbol: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Load price data from parquet
        
        Args:
            symbol: Stock symbol
            limit: Maximum rows to return
            
        Returns:
            DataFrame with price data
        """
        pattern = f"{symbol}_prices_*.parquet"
        files = sorted(self.prices_dir.glob(pattern))
        
        if not files:
            return pd.DataFrame()
        
        # Load most recent file
        df = pd.read_parquet(files[-1])
        
        if limit:
            df = df.tail(limit)
        
        return df
    
    def save_fundamentals(
        self, 
        symbol: str, 
        data_type: str, 
        df: pd.DataFrame
    ) -> str:
        """
        Save fundamental data to parquet
        
        Args:
            symbol: Stock symbol
            data_type: Type of data ('income', 'balance', 'cash', 'metrics')
            df: DataFrame with fundamental data
            
        Returns:
            Path to saved file
        """
        if df.empty:
            return ""
        
        filename = f"{symbol}_{data_type}_{datetime.now().strftime('%Y%m%d')}.parquet"
        filepath = self.fundamentals_dir / filename
        
        df_to_save = df.copy()
        df_to_save['_symbol'] = symbol
        df_to_save['_data_type'] = data_type
        df_to_save['_fetched_at'] = datetime.now().isoformat()
        
        df_to_save.to_parquet(filepath, index=False)
        return str(filepath)
    
    def load_fundamentals(
        self, 
        symbol: str, 
        data_type: str
    ) -> pd.DataFrame:
        """
        Load fundamental data from parquet
        
        Args:
            symbol: Stock symbol
            data_type: Type of data
            
        Returns:
            DataFrame with fundamental data
        """
        pattern = f"{symbol}_{data_type}_*.parquet"
        files = sorted(self.fundamentals_dir.glob(pattern))
        
        if not files:
            return pd.DataFrame()
        
        return pd.read_parquet(files[-1])
    
    def save_sec_filings(self, symbol: str, df: pd.DataFrame) -> List[str]:
        """
        Save SEC filings metadata to parquet and database
        
        Args:
            symbol: Stock symbol
            df: DataFrame with filings data
            
        Returns:
            List of saved file paths
        """
        if df.empty:
            return []
        
        filename = f"{symbol}_sec_filings_{datetime.now().strftime('%Y%m%d')}.parquet"
        filepath = self.sec_dir / filename
        
        df_to_save = df.copy()
        df_to_save['_symbol'] = symbol
        df_to_save['_fetched_at'] = datetime.now().isoformat()
        
        df_to_save.to_parquet(filepath, index=False)
        
        # Also save to database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO sec_filings
                        (symbol, filing_date, report_type, accession_number, report_url, filing_url, stored_path)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol,
                        row.get('filing_date'),
                        row.get('report_type'),
                        row.get('accession_number'),
                        row.get('report_url'),
                        row.get('filing_detail_url'),
                        str(filepath)
                    ))
                except Exception as e:
                    print(f"Error saving filing {row.get('accession_number')}: {e}")

            conn.commit()
        
        return [str(filepath)]
    
    def log_fetch(
        self, 
        symbol: str, 
        data_type: str, 
        provider: str, 
        status: str, 
        error_message: str = None,
        record_count: int = 0
    ):
        """Log a data fetch attempt"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO fetch_log
                (symbol, data_type, provider, status, error_message, record_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, data_type, provider, status, error_message, record_count))
            conn.commit()
    
    def get_fetch_history(
        self, 
        symbol: str = None, 
        limit: int = 100
    ) -> pd.DataFrame:
        """Get fetch history from database"""
        with sqlite3.connect(self.db_path) as conn:
            if symbol:
                query = "SELECT * FROM fetch_log WHERE symbol = ? ORDER BY fetch_date DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(symbol, limit))
            else:
                query = "SELECT * FROM fetch_log ORDER BY fetch_date DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(limit,))
    
    def update_watchlist(self, symbol: str, name: str = None, sector: str = None, industry: str = None):
        """Add or update symbol in watchlist table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO watchlist (symbol, name, sector, industry)
                VALUES (?, ?, ?, ?)
            """, (symbol, name, sector, industry))
            conn.commit()
    
    def get_watchlist(self) -> pd.DataFrame:
        """Get watchlist from database"""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query("SELECT * FROM watchlist WHERE is_active = 1", conn)


if __name__ == "__main__":
    # Test
    storage = DataStorage()
    print(f"Database: {storage.db_path}")
    print(f"Prices dir: {storage.prices_dir}")
    print(f"Fundamentals dir: {storage.fundamentals_dir}")
    print(f"SEC dir: {storage.sec_dir}")
