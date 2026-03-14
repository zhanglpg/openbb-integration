"""Local data storage using SQLite."""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from config import DB_PATH

logger = logging.getLogger(__name__)

# Column name validation: only allow alphanumeric and underscores
_VALID_COLUMN_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _sanitize_column_name(col: str) -> str:
    """Validate and quote a column name to prevent SQL injection."""
    if not _VALID_COLUMN_RE.match(col):
        raise ValueError(f"Invalid column name: {col!r}")
    return f'"{col}"'


class Database:
    """SQLite database for storing financial data."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self._init_db()

    def _init_db(self):
        """Initialize database tables."""
        with sqlite3.connect(self.db_path) as conn:
            # Price history
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    adj_close REAL,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                )
            """)

            # Fundamentals
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fundamentals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    market_cap REAL,
                    pe_ratio REAL,
                    pb_ratio REAL,
                    debt_to_equity REAL,
                    return_on_equity REAL,
                    payout_ratio REAL,
                    dividend_yield REAL,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, fetched_at)
                )
            """)

            # SEC filings
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sec_filings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    filing_date TEXT NOT NULL,
                    report_type TEXT NOT NULL,
                    report_url TEXT,
                    report_date TEXT,
                    accession_number TEXT,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, accession_number)
                )
            """)

            # Economic indicators
            conn.execute("""
                CREATE TABLE IF NOT EXISTS economic_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id TEXT NOT NULL,
                    date TEXT NOT NULL,
                    value REAL,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(series_id, date)
                )
            """)

            # Metadata for tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fetch_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_type TEXT NOT NULL,
                    symbol TEXT,
                    last_fetch TEXT,
                    rows_fetched INTEGER,
                    status TEXT
                )
            """)

            conn.commit()

    def save_prices(self, df: pd.DataFrame, symbol: str):
        """Save price data to database."""
        if df is None or df.empty:
            return

        df = df.copy()

        # Handle date index - OpenBB returns date as index
        if df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            # Rename the index column to 'date' if it's not already
            if df.columns[0] != "date":
                df = df.rename(columns={df.columns[0]: "date"})

        # Ensure date column exists and is string
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        df["symbol"] = symbol

        # Map common column names to our schema
        column_mapping = {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "adj_close": "adj_close",
            "adjusted_close": "adj_close",
        }

        # Rename columns if they exist
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})

        # Select only columns that exist in our schema
        schema_columns = ["symbol", "date", "open", "high", "low", "close", "volume", "adj_close"]
        available_columns = [col for col in schema_columns if col in df.columns]
        df = df[available_columns]

        with sqlite3.connect(self.db_path) as conn:
            df.to_sql("price_history", conn, if_exists="append", index=False)

    def save_fundamentals(self, df: pd.DataFrame, symbol: str):
        """Save fundamentals data to database."""
        if df is None or df.empty:
            return

        df = df.copy()
        df["symbol"] = symbol
        df["fetched_at"] = datetime.now().isoformat()

        # Get existing columns from the database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(fundamentals)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # Add any missing columns dynamically
            for col in df.columns:
                if col not in existing_columns and col not in ["id"]:
                    try:
                        safe_col = _sanitize_column_name(col)
                        conn.execute(f"ALTER TABLE fundamentals ADD COLUMN {safe_col} REAL")
                    except sqlite3.OperationalError as e:
                        if "duplicate column" in str(e).lower():
                            logger.debug("Column %s already exists in fundamentals", col)
                        else:
                            raise

            conn.commit()

        with sqlite3.connect(self.db_path) as conn:
            df.to_sql("fundamentals", conn, if_exists="append", index=False)

    def save_sec_filings(self, df: pd.DataFrame, symbol: str):
        """Save SEC filings to database."""
        if df is None or df.empty:
            return

        df = df.copy()
        df["symbol"] = symbol
        df["fetched_at"] = datetime.now().isoformat()

        # Ensure date columns are strings
        date_columns = ["filing_date", "report_date", "accepted_date"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d")

        # Get existing columns from the database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(sec_filings)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # Add any missing columns dynamically
            for col in df.columns:
                if col not in existing_columns and col not in ["id"]:
                    col_type = "TEXT" if df[col].dtype == "object" else "REAL"
                    try:
                        safe_col = _sanitize_column_name(col)
                        conn.execute(f"ALTER TABLE sec_filings ADD COLUMN {safe_col} {col_type}")
                    except sqlite3.OperationalError as e:
                        if "duplicate column" in str(e).lower():
                            logger.debug("Column %s already exists in sec_filings", col)
                        else:
                            raise

            conn.commit()

        with sqlite3.connect(self.db_path) as conn:
            df.to_sql("sec_filings", conn, if_exists="append", index=False)

    def save_economic_indicators(self, df: pd.DataFrame, series_id: str):
        """Save economic indicator data to database."""
        if df is None or df.empty:
            return

        df = df.copy()

        # Handle date index
        if df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if df.columns[0] != "date":
                df = df.rename(columns={df.columns[0]: "date"})

        # Ensure date column exists and is string
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        df["series_id"] = series_id
        df["fetched_at"] = datetime.now().isoformat()

        # Get existing columns from the database
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("PRAGMA table_info(economic_indicators)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # Add any missing columns dynamically (quote column names for reserved keywords)
            for col in df.columns:
                if col not in existing_columns and col not in ["id"]:
                    col_type = "TEXT" if df[col].dtype == "object" else "REAL"
                    try:
                        safe_col = _sanitize_column_name(col)
                        conn.execute(
                            f"ALTER TABLE economic_indicators ADD COLUMN {safe_col} {col_type}"
                        )
                        conn.commit()
                    except sqlite3.OperationalError as e:
                        if "duplicate column" in str(e).lower():
                            logger.debug("Column %s already exists in economic_indicators", col)
                        else:
                            raise

        with sqlite3.connect(self.db_path) as conn:
            # Delete existing data for this series to avoid duplicates
            conn.execute("DELETE FROM economic_indicators WHERE series_id = ?", (series_id,))
            conn.commit()
            df.to_sql("economic_indicators", conn, if_exists="append", index=False)

    def get_latest_prices(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Get latest price data for a symbol."""
        query = """
            SELECT * FROM price_history
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(symbol, days))

    def get_all_symbols(self) -> list:
        """Get all symbols in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT DISTINCT symbol FROM price_history")
            return [row[0] for row in cursor.fetchall()]

    def update_metadata(
        self, data_type: str, symbol: Optional[str], rows_fetched: int, status: str = "success"
    ):
        """Update fetch metadata."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fetch_metadata (data_type, symbol, last_fetch, rows_fetched, status)
                VALUES (?, ?, ?, ?, ?)
            """,
                (data_type, symbol, datetime.now().isoformat(), rows_fetched, status),
            )
            conn.commit()


if __name__ == "__main__":
    # Test database initialization
    db = Database()
    print(f"Database initialized at: {db.db_path}")
    print("Tables created successfully")
