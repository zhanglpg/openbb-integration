"""Local data storage using SQLite."""

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

from config import DB_PATH

logger = logging.getLogger(__name__)

# Current schema version — bump this when making incompatible changes.
# Future code can compare this against the DB value to decide whether to migrate.
SCHEMA_VERSION = 3


def _normalize_date_index(df: pd.DataFrame) -> pd.DataFrame:
    """Reset a DatetimeIndex or date-named index to a 'date' column formatted as YYYY-MM-DD."""
    if df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()
        if df.columns[0] != "date":
            df = df.rename(columns={df.columns[0]: "date"})
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


class Database:
    """SQLite database for storing financial data."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or DB_PATH
        self._init_db()

    def _init_db(self):
        """Initialize database tables, indexes, and schema version."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")

            # ----------------------------------------------------------
            # Schema version tracking
            # ----------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL,
                    applied_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Record current version if the table is empty
            cursor = conn.execute("SELECT COUNT(*) FROM schema_version")
            if cursor.fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)",
                    (SCHEMA_VERSION,),
                )

            # ----------------------------------------------------------
            # Price history
            # ----------------------------------------------------------
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

            # ----------------------------------------------------------
            # Fundamentals — fixed columns, snapshot_date for dedup
            # ----------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fundamentals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    market_cap REAL,
                    pe_ratio REAL,
                    pb_ratio REAL,
                    debt_to_equity REAL,
                    return_on_equity REAL,
                    payout_ratio REAL,
                    dividend_yield REAL,
                    revenue REAL,
                    net_income REAL,
                    eps REAL,
                    free_cash_flow REAL,
                    reporting_currency TEXT,
                    trading_currency TEXT,
                    extra_data TEXT,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, snapshot_date)
                )
            """)

            # Schema v3 migration: add currency columns to existing tables
            existing_cols = {
                row[1] for row in conn.execute("PRAGMA table_info(fundamentals)").fetchall()
            }
            if "reporting_currency" not in existing_cols:
                conn.execute("ALTER TABLE fundamentals ADD COLUMN reporting_currency TEXT")
            if "trading_currency" not in existing_cols:
                conn.execute("ALTER TABLE fundamentals ADD COLUMN trading_currency TEXT")

            # ----------------------------------------------------------
            # SEC filings — fixed columns, accession_number NOT NULL
            # ----------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sec_filings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    filing_date TEXT NOT NULL,
                    report_type TEXT NOT NULL,
                    accession_number TEXT NOT NULL,
                    report_url TEXT,
                    report_date TEXT,
                    filing_detail_url TEXT,
                    primary_doc TEXT,
                    primary_doc_description TEXT,
                    extra_data TEXT,
                    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, accession_number)
                )
            """)

            # ----------------------------------------------------------
            # Economic indicators — fixed columns
            # ----------------------------------------------------------
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

            # ----------------------------------------------------------
            # Watchlist
            # ----------------------------------------------------------
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

            # ----------------------------------------------------------
            # Fetch log — single table (replaces both fetch_metadata
            # and the old fetch_log)
            # ----------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fetch_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_type TEXT NOT NULL,
                    symbol TEXT,
                    provider TEXT,
                    fetch_date TEXT DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    record_count INTEGER DEFAULT 0
                )
            """)

            # ----------------------------------------------------------
            # Holdings — shares per symbol for portfolio allocation
            # ----------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS holdings (
                    symbol TEXT PRIMARY KEY,
                    shares REAL NOT NULL DEFAULT 0,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ----------------------------------------------------------
            # Research notes
            # ----------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS research_notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    note TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_notes_symbol ON research_notes(symbol)"
            )

            # ----------------------------------------------------------
            # Indexes
            # ----------------------------------------------------------
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_price_history_symbol ON price_history(symbol)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(date)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fundamentals_symbol ON fundamentals(symbol)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sec_filings_symbol ON sec_filings(symbol)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_economic_indicators_series_id"
                " ON economic_indicators(series_id)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fetch_log_symbol ON fetch_log(symbol)")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_fetch_log_data_type ON fetch_log(data_type)"
            )

            conn.commit()

    # ==================================================================
    # Price history
    # ==================================================================

    def _validate_price_rows(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Drop rows with nulls in required columns or non-positive close prices."""
        before_len = len(df)
        df = df.dropna(subset=["symbol", "date", "close"])
        if len(df) < before_len:
            logger.warning(
                "Dropped %d rows with null required values for %s", before_len - len(df), symbol
            )
        invalid_mask = df["close"] <= 0
        if invalid_mask.any():
            logger.warning(
                "Dropped %d rows with non-positive close price for %s", invalid_mask.sum(), symbol
            )
            df = df[~invalid_mask]
        return df

    def save_prices(self, df: pd.DataFrame, symbol: str):
        """Save price data to database using upsert to handle re-runs."""
        if df is None or df.empty:
            return

        df = _normalize_date_index(df.copy())
        df["symbol"] = symbol

        if "adjusted_close" in df.columns and "adj_close" not in df.columns:
            df = df.rename(columns={"adjusted_close": "adj_close"})

        schema_columns = ["symbol", "date", "open", "high", "low", "close", "volume", "adj_close"]
        available_columns = [col for col in schema_columns if col in df.columns]
        df = df[available_columns]

        missing = {"symbol", "date", "close"} - set(df.columns)
        if missing:
            logger.warning("Price data for %s missing required columns: %s", symbol, missing)
            return

        df = self._validate_price_rows(df, symbol)
        if df.empty:
            return

        with sqlite3.connect(self.db_path) as conn:
            for _, row in df.iterrows():
                cols = list(available_columns)
                placeholders = ", ".join(["?"] * len(cols))
                col_names = ", ".join(cols)
                values = [row[c] for c in cols]
                conn.execute(
                    f"INSERT OR REPLACE INTO price_history ({col_names}) VALUES ({placeholders})",
                    values,
                )
            conn.commit()

    # ==================================================================
    # Fundamentals
    # ==================================================================

    # Columns we explicitly track. Anything else goes into extra_data.
    _FUNDAMENTALS_KNOWN_COLS = {
        "market_cap",
        "pe_ratio",
        "pb_ratio",
        "debt_to_equity",
        "return_on_equity",
        "payout_ratio",
        "dividend_yield",
        "revenue",
        "net_income",
        "eps",
        "free_cash_flow",
        "reporting_currency",
        "trading_currency",
    }

    @staticmethod
    def _split_known_extra(row, known_cols: set, skip_cols: set) -> tuple[dict, dict]:
        """Split a DataFrame row into known column values and extra data."""
        known_values = {}
        extra = {}
        for col, val in row.items():
            if col in skip_cols:
                continue
            if col in known_cols:
                known_values[col] = None if pd.isna(val) else val
            elif not pd.isna(val):
                extra[col] = val
        return known_values, extra

    @staticmethod
    def _upsert_row(
        conn, table: str, prefix_cols: list[str], prefix_vals: list, known_values: dict, extra: dict
    ):
        """Execute an INSERT OR REPLACE for a single row with known + extra_data columns."""
        cols = prefix_cols + list(known_values.keys()) + ["extra_data"]
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        values = prefix_vals + list(known_values.values()) + [json.dumps(extra) if extra else None]
        conn.execute(
            f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})",
            values,
        )

    def save_fundamentals(self, df: pd.DataFrame, symbol: str):
        """Save fundamentals data with fixed schema.  Extra columns are
        stored as JSON in ``extra_data``."""
        if df is None or df.empty:
            return

        df = df.copy()
        df["symbol"] = symbol
        df["snapshot_date"] = datetime.now().strftime("%Y-%m-%d")

        skip = {"symbol", "snapshot_date", "id", "fetched_at"}
        with sqlite3.connect(self.db_path) as conn:
            for _, row in df.iterrows():
                known, extra = self._split_known_extra(row, self._FUNDAMENTALS_KNOWN_COLS, skip)
                self._upsert_row(
                    conn,
                    "fundamentals",
                    ["symbol", "snapshot_date"],
                    [symbol, row["snapshot_date"]],
                    known,
                    extra,
                )
            conn.commit()

    # ==================================================================
    # SEC filings
    # ==================================================================

    _SEC_KNOWN_COLS = {
        "filing_date",
        "report_type",
        "accession_number",
        "report_url",
        "report_date",
        "filing_detail_url",
        "primary_doc",
        "primary_doc_description",
    }

    def _clean_sec_dates(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Coerce SEC date columns to strings and drop rows missing required fields."""
        for col in ["filing_date", "report_date", "accepted_date"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
                df[col] = df[col].replace("NaT", None)

        for col, label in [
            ("filing_date", "invalid filing_date"),
            ("accession_number", "null accession_number"),
        ]:
            if col not in df.columns:
                if col == "accession_number":
                    logger.warning(
                        "SEC filings for %s missing accession_number column, skipping", symbol
                    )
                    return pd.DataFrame()
                continue
            before_len = len(df)
            df = df.dropna(subset=[col])
            if len(df) < before_len:
                logger.warning(
                    "Dropped %d rows with %s for %s", before_len - len(df), label, symbol
                )
        return df

    def save_sec_filings(self, df: pd.DataFrame, symbol: str):
        """Save SEC filings with fixed schema.  Extra columns stored as JSON."""
        if df is None or df.empty:
            return

        df = df.copy()
        df["symbol"] = symbol
        df = self._clean_sec_dates(df, symbol)
        if df.empty:
            return

        skip = {"symbol", "id", "fetched_at"}
        with sqlite3.connect(self.db_path) as conn:
            for _, row in df.iterrows():
                known, extra = self._split_known_extra(row, self._SEC_KNOWN_COLS, skip)
                self._upsert_row(conn, "sec_filings", ["symbol"], [symbol], known, extra)
            conn.commit()

    # ==================================================================
    # Economic indicators
    # ==================================================================

    def _resolve_value_column(self, df: pd.DataFrame, series_id: str) -> pd.DataFrame:
        """Rename first numeric column to 'value' if 'value' is missing."""
        if "value" in df.columns:
            return df
        exclude = {"series_id", "fetched_at", "date", "id"}
        numeric = [c for c in df.select_dtypes(include="number").columns if c not in exclude]
        if numeric:
            logger.info("Column 'value' not found for %s; using '%s'", series_id, numeric[0])
            df = df.rename(columns={numeric[0]: "value"})
        return df

    def save_economic_indicators(self, df: pd.DataFrame, series_id: str):
        """Save economic indicator data using INSERT OR REPLACE (atomic)."""
        if df is None or df.empty:
            return

        df = self._resolve_value_column(_normalize_date_index(df.copy()), series_id)

        df["series_id"] = series_id
        now = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            for _, row in df.iterrows():
                date_val = row.get("date")
                value_val = row.get("value")
                if pd.isna(date_val):
                    continue
                conn.execute(
                    "INSERT OR REPLACE INTO economic_indicators"
                    " (series_id, date, value, fetched_at) VALUES (?, ?, ?, ?)",
                    (series_id, date_val, None if pd.isna(value_val) else value_val, now),
                )
            conn.commit()

    # ==================================================================
    # Read methods
    # ==================================================================

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

    def get_price_history_by_date(
        self, symbol: str, start_date: str, end_date: str = None
    ) -> pd.DataFrame:
        """Get price history for a symbol within a date range.

        Args:
            symbol: Ticker symbol.
            start_date: ISO date string (YYYY-MM-DD).
            end_date: ISO date string, defaults to today.

        Returns:
            DataFrame with columns [date, open, high, low, close, volume, adj_close],
            sorted by date ascending.
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        query = """
            SELECT date, open, high, low, close, volume, adj_close
            FROM price_history
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))

    def get_latest_prices_batch(self, symbols: List[str], days: int = 1) -> pd.DataFrame:
        """Get the latest price row per symbol in one query."""
        if not symbols:
            return pd.DataFrame()

        placeholders = ", ".join(["?"] * len(symbols))
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

    def get_latest_economic_indicators(
        self, series_ids: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get the most recent value for each economic indicator series."""
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

    def get_price_history_batch(self, symbols: List[str], days: int = 90) -> pd.DataFrame:
        """Get price history for multiple symbols — last N rows per symbol."""
        if not symbols:
            return pd.DataFrame()

        placeholders = ", ".join(["?"] * len(symbols))
        query = f"""
            SELECT symbol, date, open, high, low, close, volume, adj_close
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
                FROM price_history
                WHERE symbol IN ({placeholders})
            )
            WHERE rn <= ?
            ORDER BY symbol, date
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=symbols + [days])

    def get_all_fundamentals(self) -> pd.DataFrame:
        """Get the latest fundamentals snapshot for all symbols."""
        query = """
            SELECT f.*
            FROM fundamentals f
            INNER JOIN (
                SELECT symbol, MAX(snapshot_date) as max_date
                FROM fundamentals
                GROUP BY symbol
            ) latest ON f.symbol = latest.symbol AND f.snapshot_date = latest.max_date
            ORDER BY f.symbol
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn)

    def get_economic_indicator_history(self, series_id: str, days: int = 365) -> pd.DataFrame:
        """Get time series for a single FRED series — last N rows."""
        query = """
            SELECT series_id, date, value
            FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY date DESC) as rn
                FROM economic_indicators
                WHERE series_id = ?
            )
            WHERE rn <= ?
            ORDER BY date
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(series_id, days))

    def get_all_symbols(self) -> list:
        """Get all symbols in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT DISTINCT symbol FROM price_history")
            return [row[0] for row in cursor.fetchall()]

    # ==================================================================
    # Fetch logging (single table — replaces old fetch_metadata + fetch_log)
    # ==================================================================

    def update_metadata(
        self, data_type: str, symbol: Optional[str], rows_fetched: int, status: str = "success"
    ):
        """Log a fetch operation (alias kept for backward compatibility)."""
        self.log_fetch(
            symbol or "",
            data_type,
            provider="",
            status=status,
            record_count=rows_fetched,
        )

    def log_fetch(
        self,
        symbol: str,
        data_type: str,
        provider: str = "",
        status: str = "success",
        error_message: str = None,
        record_count: int = 0,
    ):
        """Log a data fetch attempt."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fetch_log
                (data_type, symbol, provider, status, error_message, record_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (data_type, symbol, provider, status, error_message, record_count),
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

    # ==================================================================
    # Watchlist
    # ==================================================================

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

    # ==================================================================
    # Research notes
    # ==================================================================

    def save_note(self, symbol: str, note: str):
        """Save a research note for a symbol (appends, does not overwrite)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO research_notes (symbol, note) VALUES (?, ?)",
                (symbol, note),
            )
            conn.commit()

    def get_notes(self, symbol: str, limit: int = 20) -> pd.DataFrame:
        """Get research notes for a symbol, most recent first."""
        query = """
            SELECT id, symbol, note, updated_at
            FROM research_notes
            WHERE symbol = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
        """
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(symbol, limit))

    def delete_note(self, note_id: int):
        """Delete a research note by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM research_notes WHERE id = ?", (int(note_id),))
            conn.commit()

    # ------------------------------------------------------------------
    # Holdings
    # ------------------------------------------------------------------

    def get_holdings(self) -> pd.DataFrame:
        """Get all holdings (symbol + shares)."""
        query = "SELECT symbol, shares FROM holdings WHERE shares > 0 ORDER BY symbol"
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn)

    def update_holding(self, symbol: str, shares: float):
        """Insert or update shares for a symbol."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO holdings (symbol, shares, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(symbol) DO UPDATE SET
                       shares = excluded.shares,
                       updated_at = excluded.updated_at""",
                (symbol, shares),
            )
            conn.commit()


if __name__ == "__main__":
    db = Database()
    print(f"Database initialized at: {db.db_path}")
    print(f"Schema version: {SCHEMA_VERSION}")
    print("Tables created successfully")
