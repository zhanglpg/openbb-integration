"""Local data storage using SQLite."""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

from config import DB_PATH, DATA_DIR

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
        """Initialize database tables and indexes."""
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

            # Watchlist (consolidated from storage.py)
            conn.execute("""
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

            # Fetch log (consolidated from storage.py)
            conn.execute("""
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

            # Indexes for query performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_symbol ON price_history(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol ON fundamentals(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sec_filings_symbol ON sec_filings(symbol)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_economic_indicators_series_id ON economic_indicators(series_id)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fetch_metadata_symbol ON fetch_metadata(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fetch_log_symbol ON fetch_log(symbol)")

            conn.commit()

    def save_prices(self, df: pd.DataFrame, symbol: str):
        """Save price data to database using upsert to handle re-runs."""
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

        # Validate required columns
        required = {"symbol", "date", "close"}
        missing = required - set(df.columns)
        if missing:
            logger.warning("Price data for %s missing required columns: %s", symbol, missing)
            return

        # Drop rows with null required values
        before_len = len(df)
        df = df.dropna(subset=["symbol", "date", "close"])
        if len(df) < before_len:
            logger.warning(
                "Dropped %d rows with null required values for %s", before_len - len(df), symbol
            )

        # Validate price values (drop rows with non-positive close)
        invalid_mask = df["close"] <= 0
        if invalid_mask.any():
            logger.warning("Dropped %d rows with non-positive close price for %s", invalid_mask.sum(), symbol)
            df = df[~invalid_mask]

        if df.empty:
            return

        # Use INSERT OR REPLACE for idempotent pipeline re-runs
        with sqlite3.connect(self.db_path) as conn:
            for _, row in df.iterrows():
                cols = [c for c in available_columns]
                placeholders = ", ".join(["?"] * len(cols))
                col_names = ", ".join(cols)
                values = [row[c] for c in cols]
                conn.execute(
                    f"INSERT OR REPLACE INTO price_history ({col_names}) VALUES ({placeholders})",
                    values,
                )
            conn.commit()

    def save_fundamentals(self, df: pd.DataFrame, symbol: str):
        """Save fundamentals data to database."""
        if df is None or df.empty:
            return

        df = df.copy()
        df["symbol"] = symbol
        df["fetched_at"] = datetime.now().isoformat()

        # Validate symbol is present
        if "symbol" not in df.columns or df["symbol"].isna().all():
            logger.warning("Fundamentals data missing symbol, skipping save")
            return

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

        # Validate accession_number is present for UNIQUE constraint
        if "accession_number" in df.columns:
            before_len = len(df)
            df = df.dropna(subset=["accession_number"])
            if len(df) < before_len:
                logger.warning(
                    "Dropped %d rows with null accession_number for %s",
                    before_len - len(df),
                    symbol,
                )

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

    def get_latest_prices_batch(self, symbols: List[str], days: int = 1) -> pd.DataFrame:
        """Get latest price data for multiple symbols in one query.

        For each symbol, returns the most recent `days` rows.
        """
        if not symbols:
            return pd.DataFrame()

        placeholders = ", ".join(["?"] * len(symbols))
        query = f"""
            SELECT * FROM price_history
            WHERE symbol IN ({placeholders})
            AND date >= (
                SELECT MAX(date) FROM price_history
                WHERE symbol = price_history.symbol
            )
            ORDER BY symbol, date DESC
        """

        # Simpler approach: get the latest row per symbol
        # Using a subquery to find max date per symbol
        query = f"""
            SELECT p.*
            FROM price_history p
            INNER JOIN (
                SELECT symbol, MAX(date) as max_date
                FROM price_history
                WHERE symbol IN ({placeholders})
                GROUP BY symbol
            ) latest ON p.symbol = latest.symbol AND p.date = latest.max_date
            ORDER BY p.symbol
        """

        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=symbols)

    def get_latest_prices_batch_with_previous(self, symbols: List[str]) -> pd.DataFrame:
        """Get the two most recent prices per symbol for change calculation."""
        if not symbols:
            return pd.DataFrame()

        placeholders = ", ".join(["?"] * len(symbols))
        # Use ROW_NUMBER to get the 2 most recent rows per symbol
        query = f"""
            SELECT symbol, date, open, high, low, close, volume, adj_close
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
                FROM price_history
                WHERE symbol IN ({placeholders})
            )
            WHERE rn <= 2
            ORDER BY symbol, date DESC
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=symbols)

    def get_latest_economic_indicators(self, series_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """Get the most recent value for each economic indicator series.

        Args:
            series_ids: Optional list of series IDs to filter. If None, returns all.
        """
        if series_ids:
            placeholders = ", ".join(["?"] * len(series_ids))
            query = f"""
                SELECT e1.series_id, e1.date, e1.value
                FROM economic_indicators e1
                INNER JOIN (
                    SELECT series_id, MAX(date) as max_date
                    FROM economic_indicators
                    WHERE series_id IN ({placeholders})
                    GROUP BY series_id
                ) e2 ON e1.series_id = e2.series_id AND e1.date = e2.max_date
                ORDER BY e1.series_id
            """
            params = series_ids
        else:
            query = """
                SELECT e1.series_id, e1.date, e1.value
                FROM economic_indicators e1
                INNER JOIN (
                    SELECT series_id, MAX(date) as max_date
                    FROM economic_indicators
                    GROUP BY series_id
                ) e2 ON e1.series_id = e2.series_id AND e1.date = e2.max_date
                ORDER BY e1.series_id
            """
            params = []

        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=params)

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

    # --- Methods consolidated from DataStorage ---

    def log_fetch(
        self,
        symbol: str,
        data_type: str,
        provider: str,
        status: str,
        error_message: str = None,
        record_count: int = 0,
    ):
        """Log a data fetch attempt."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fetch_log
                (symbol, data_type, provider, status, error_message, record_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (symbol, data_type, provider, status, error_message, record_count),
            )
            conn.commit()

    def get_fetch_history(self, symbol: str = None, limit: int = 100) -> pd.DataFrame:
        """Get fetch history from database."""
        with sqlite3.connect(self.db_path) as conn:
            if symbol:
                query = "SELECT * FROM fetch_log WHERE symbol = ? ORDER BY fetch_date DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(symbol, limit))
            else:
                query = "SELECT * FROM fetch_log ORDER BY fetch_date DESC LIMIT ?"
                return pd.read_sql_query(query, conn, params=(limit,))

    def update_watchlist(
        self, symbol: str, name: str = None, sector: str = None, industry: str = None
    ):
        """Add or update symbol in watchlist table."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO watchlist (symbol, name, sector, industry)
                VALUES (?, ?, ?, ?)
            """,
                (symbol, name, sector, industry),
            )
            conn.commit()

    def get_watchlist(self) -> pd.DataFrame:
        """Get watchlist from database."""
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query("SELECT * FROM watchlist WHERE is_active = 1", conn)


if __name__ == "__main__":
    # Test database initialization
    db = Database()
    print(f"Database initialized at: {db.db_path}")
    print("Tables created successfully")
