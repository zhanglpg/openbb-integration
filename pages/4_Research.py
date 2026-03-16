"""
Research Dashboard Page
Deep-dive analysis for individual symbols with peer comparison.
"""

import glob  # noqa: I001
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from shared import get_db, render_sidebar_controls  # must be first: adds src/ to sys.path

from analysis import (
    compute_financial_ratios,
    compute_growth_rates,
    compute_historical_valuations,
    compute_macro_snapshot,
    compute_portfolio_risk,
    compute_price_technicals,
    compute_ttm,
    compute_valuation_screen,
    normalize_price_series,
    summarize_insider_activity,
)
from config import WATCHLIST
from fetcher import DataFetcher
from research import analyze_symbol_deep, assess_macro_risks, compare_peers, screen_opportunities

ALL_SYMBOLS = sorted(set(sym for symbols in WATCHLIST.values() for sym in symbols))


def _find_peer_category(symbol: str) -> tuple[str | None, list[str]]:
    """Return (category_name, peer_symbols) for the symbol's watchlist group."""
    for category, symbols in WATCHLIST.items():
        if symbol in symbols:
            return category, symbols
    return None, [symbol]


def _load_sec_filings(symbol: str) -> pd.DataFrame:
    """Load SEC filings from parquet (same pattern as dashboard.py)."""
    sec_dir = Path(__file__).parent.parent / "data" / "sec"
    pattern = str(sec_dir / f"{symbol}_sec_filings_*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(files[-1])


@st.cache_data(ttl=300)
def run_deep_analysis(symbol: str, _db):
    """Cached deep analysis for a symbol with peer context."""
    category, peers = _find_peer_category(symbol)

    # Fetch data from DB
    prices_df = _db.get_price_history_batch(peers, days=90)
    fundamentals_df = _db.get_all_fundamentals()
    sec_df = _load_sec_filings(symbol)

    # Compute technicals for all peers
    technicals = {}
    for sym in peers:
        sym_prices = (
            prices_df[prices_df["symbol"] == sym] if not prices_df.empty else pd.DataFrame()
        )
        technicals[sym] = compute_price_technicals(sym_prices, sym)

    # Valuation screen
    peer_fund_df = (
        fundamentals_df[fundamentals_df["symbol"].isin(peers)]
        if not fundamentals_df.empty
        else pd.DataFrame()
    )
    valuations = compute_valuation_screen(peer_fund_df)

    # Risk
    risk_data = compute_portfolio_risk(prices_df, WATCHLIST)

    # Peer comparison
    peer_context = compare_peers(peers, technicals, valuations, risk_data)

    # Symbol fundamentals as dict
    sym_fund = {}
    if not fundamentals_df.empty:
        sym_row = fundamentals_df[fundamentals_df["symbol"] == symbol]
        if not sym_row.empty:
            sym_fund = sym_row.iloc[0].to_dict()

    # SEC filings as list of dicts
    sec_list = sec_df.to_dict(orient="records") if not sec_df.empty else []

    # Deep analysis
    deep = analyze_symbol_deep(
        symbol,
        technicals.get(symbol, {"error": "no data"}),
        sym_fund,
        sec_list,
        peer_context=peer_context,
    )

    # Macro risk assessment
    macro_risk = None
    try:
        key_series = ["VIXCLS", "DGS10", "T10Y2Y", "FEDFUNDS"]
        indicator_histories = {}
        for sid in key_series:
            idf = _db.get_economic_indicator_history(sid)
            if idf is not None and not idf.empty:
                indicator_histories[sid] = idf
        if indicator_histories:
            macro_snap = compute_macro_snapshot(indicator_histories)
            macro_risk = assess_macro_risks(macro_snap, risk_data)
    except Exception:
        pass

    # Opportunity screening
    opportunities = screen_opportunities(valuations, technicals)

    return {
        "deep": deep,
        "peer_context": peer_context,
        "technicals": technicals,
        "category": category,
        "peers": peers,
        "macro_risk": macro_risk,
        "opportunities": opportunities,
    }


# =========================================================================
# Cached financial statement fetchers
# =========================================================================


@st.cache_data(ttl=86400)
def fetch_reporting_currency(symbol: str) -> str:
    """Get the financial reporting currency for a symbol via yfinance."""
    try:
        import yfinance as yf

        info = yf.Ticker(symbol).info
        return info.get("financialCurrency") or info.get("currency") or "USD"
    except Exception:
        return "USD"


@st.cache_data(ttl=86400)
def fetch_trading_currency(symbol: str) -> str:
    """Get the trading currency for a symbol via yfinance."""
    try:
        import yfinance as yf

        info = yf.Ticker(symbol).info
        return info.get("currency") or "USD"
    except Exception:
        return "USD"


@st.cache_data(ttl=86400)
def fetch_fx_rate(from_currency: str, to_currency: str) -> float:
    """Get exchange rate from_currency -> to_currency (how many from per 1 to).

    E.g. fetch_fx_rate("CNY", "USD") returns ~7.2 (7.2 CNY per 1 USD).
    Returns 1.0 if currencies are the same or lookup fails.
    """
    if from_currency == to_currency:
        return 1.0
    try:
        import yfinance as yf

        # yfinance format: "CNYUSD=X" gives CNY per USD
        pair = f"{from_currency}{to_currency}=X"
        ticker = yf.Ticker(pair)
        hist = ticker.history(period="5d")
        if not hist.empty:
            rate = float(hist["Close"].iloc[-1])
            if rate > 0:
                # We need from_currency per to_currency
                # CNYUSD=X gives "how many USD per 1 CNY" (e.g. 0.14)
                # We want CNY per USD (e.g. 7.2) = 1/rate
                return round(1.0 / rate, 4)
        # Try reverse pair
        pair_rev = f"{to_currency}{from_currency}=X"
        ticker_rev = yf.Ticker(pair_rev)
        hist_rev = ticker_rev.history(period="5d")
        if not hist_rev.empty:
            rate_rev = float(hist_rev["Close"].iloc[-1])
            if rate_rev > 0:
                return round(rate_rev, 4)
    except Exception:
        pass
    return 1.0


@st.cache_data(ttl=3600, show_spinner="Loading income statement...")
def fetch_income(symbol: str, period: str = "annual") -> pd.DataFrame:
    return DataFetcher().fetch_income_statement(symbol, period=period)


@st.cache_data(ttl=3600, show_spinner="Loading balance sheet...")
def fetch_balance(symbol: str, period: str = "annual") -> pd.DataFrame:
    return DataFetcher().fetch_balance_sheet(symbol, period=period)


@st.cache_data(ttl=3600, show_spinner="Loading cash flow...")
def fetch_cashflow(symbol: str, period: str = "annual") -> pd.DataFrame:
    return DataFetcher().fetch_cash_flow(symbol, period=period)


@st.cache_data(ttl=3600, show_spinner="Loading insider trades...")
def fetch_insider_trades(symbol: str) -> pd.DataFrame:
    return DataFetcher().fetch_insider_trades(symbol)


@st.cache_data(ttl=3600, show_spinner="Loading institutional holders...")
def fetch_institutional(symbol: str) -> pd.DataFrame:
    return DataFetcher().fetch_institutional_holders(symbol)


# =========================================================================
# Formatting helpers
# =========================================================================


def _fmt(value, fmt=".2f", prefix="", suffix="", fallback="N/A"):
    """Format a numeric value, returning fallback for None/NaN."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    return f"{prefix}{value:{fmt}}{suffix}"


def _fmt_large(value, fallback="N/A", currency="$"):
    """Format large numbers (market cap, revenue, FCF) with B/M suffix."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    sign = "-" if value < 0 else ""
    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"{sign}{currency}{abs_val / 1e12:.2f}T"
    if abs_val >= 1e9:
        return f"{sign}{currency}{abs_val / 1e9:.2f}B"
    if abs_val >= 1e6:
        return f"{sign}{currency}{abs_val / 1e6:.1f}M"
    return f"{sign}{currency}{abs_val:,.0f}"


def _period_label(dt):
    """Format a period_ending date as 'FY2024' style label."""
    if pd.isna(dt):
        return "N/A"
    if hasattr(dt, "strftime"):
        return dt.strftime("%b %Y")
    return str(dt)


def _get_col(obj, name, default=None):
    """Safely get a value from a DataFrame row (Series) or a column from a DataFrame."""
    try:
        if name in obj.index if isinstance(obj, pd.Series) else name in obj.columns:
            return obj[name]
    except (AttributeError, KeyError):
        pass
    return default


# =========================================================================
# Financial Statements tab rendering
# =========================================================================


def _render_income_statement(symbol, df=None, cf_df=None, cur="$"):
    """Render income statement trends."""
    st.subheader("Income Statement")
    if df is None:
        df = fetch_income(symbol)
    if df.empty or "period_ending" not in df.columns:
        st.info("No income statement data available for this symbol")
        return

    df = df.sort_values("period_ending").copy()
    df["period_ending"] = pd.to_datetime(df["period_ending"])

    # Merge SBC from cash flow to compute adjusted EBITDA
    if cf_df is None:
        cf_df = fetch_cashflow(symbol)
    if (
        not cf_df.empty
        and "period_ending" in cf_df.columns
        and "stock_based_compensation" in cf_df.columns
    ):
        sbc = cf_df[["period_ending", "stock_based_compensation"]].copy()
        sbc["period_ending"] = pd.to_datetime(sbc["period_ending"])
        df = df.merge(sbc, on="period_ending", how="left")

    # Compute adjusted EBITDA = GAAP EBITDA + SBC
    if "ebitda" in df.columns and "stock_based_compensation" in df.columns:
        df["adjusted_ebitda"] = df["ebitda"] + df["stock_based_compensation"].fillna(0)

    labels = df["period_ending"].apply(_period_label)

    # Metrics — 2 rows of 3 columns to avoid truncation
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None
    has_adj = "adjusted_ebitda" in df.columns

    r1a, r1b, r1c = st.columns(3)
    r1a.metric("Revenue", _fmt_large(_get_col(latest, "total_revenue"), currency=cur))
    if prev is not None and "total_revenue" in df.columns:
        rev_now = _get_col(latest, "total_revenue")
        rev_prev = _get_col(prev, "total_revenue")
        if rev_now and rev_prev and rev_prev != 0:
            growth = (rev_now - rev_prev) / abs(rev_prev) * 100
            r1b.metric("Rev Growth", f"{growth:+.1f}%")
    r1c.metric(
        "EPS",
        _fmt(_get_col(latest, "diluted_earnings_per_share"), ".2f", prefix=cur),
    )

    if has_adj:
        r2a, r2b, r2c = st.columns(3)
        r2a.metric("EBITDA", _fmt_large(_get_col(latest, "ebitda"), currency=cur))
        r2b.metric("EBITDA (Adj)", _fmt_large(_get_col(latest, "adjusted_ebitda"), currency=cur))
    else:
        r2a, _, _ = st.columns(3)
        r2a.metric("EBITDA", _fmt_large(_get_col(latest, "ebitda"), currency=cur))

    # Row 1: Revenue vs Net Income | Margin trends
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        fig = go.Figure()
        if "total_revenue" in df.columns:
            fig.add_trace(
                go.Bar(
                    x=labels,
                    y=df["total_revenue"],
                    name="Revenue",
                    marker_color="#2196F3",
                )
            )
        if "net_income" in df.columns:
            fig.add_trace(
                go.Bar(
                    x=labels,
                    y=df["net_income"],
                    name="Net Income",
                    marker_color="#26a69a",
                )
            )
        fig.update_layout(
            title="Revenue vs Net Income",
            barmode="group",
            height=350,
            margin=dict(l=40, r=20, t=40, b=30),
        )
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        fig = go.Figure()
        for col, name, color in [
            ("gross_profit", "Gross Margin", "#2196F3"),
            ("operating_income", "Operating Margin", "#FF9800"),
            ("net_income", "Net Margin", "#26a69a"),
        ]:
            if col in df.columns and "total_revenue" in df.columns:
                margin = (df[col] / df["total_revenue"].replace(0, pd.NA)) * 100
                fig.add_trace(
                    go.Scatter(
                        x=labels,
                        y=margin,
                        name=name,
                        mode="lines+markers",
                        line=dict(color=color),
                    )
                )
        fig.update_layout(
            title="Margin Trends (%)",
            height=350,
            margin=dict(l=40, r=20, t=40, b=30),
            yaxis_title="%",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Row 2: EBITDA (GAAP vs Adjusted) | SBC
    has_adj = "adjusted_ebitda" in df.columns
    if "ebitda" in df.columns:
        ebitda_col1, ebitda_col2 = st.columns(2)
        with ebitda_col1:
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=labels,
                    y=df["ebitda"],
                    name="EBITDA (GAAP)",
                    marker_color="#2196F3",
                )
            )
            if has_adj:
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=df["adjusted_ebitda"],
                        name="EBITDA (Adjusted)",
                        marker_color="#FF9800",
                    )
                )
            fig.update_layout(
                title="EBITDA: GAAP vs Adjusted",
                barmode="group",
                height=350,
                margin=dict(l=40, r=20, t=40, b=30),
            )
            st.plotly_chart(fig, use_container_width=True)

        with ebitda_col2:
            if "stock_based_compensation" in df.columns:
                fig = go.Figure()
                fig.add_trace(
                    go.Bar(
                        x=labels,
                        y=df["stock_based_compensation"],
                        name="SBC",
                        marker_color="#9C27B0",
                    )
                )
                if "total_revenue" in df.columns:
                    sbc_pct = (
                        df["stock_based_compensation"] / df["total_revenue"].replace(0, pd.NA) * 100
                    )
                    fig.add_trace(
                        go.Scatter(
                            x=labels,
                            y=sbc_pct,
                            name="SBC % Rev",
                            mode="lines+markers",
                            line=dict(color="#E91E63"),
                            yaxis="y2",
                        )
                    )
                fig.update_layout(
                    title="Stock-Based Compensation",
                    height=350,
                    margin=dict(l=40, r=20, t=40, b=30),
                    yaxis2=dict(title="%", overlaying="y", side="right"),
                )
                st.plotly_chart(fig, use_container_width=True)

    # Data table
    with st.expander("Income Statement Data"):
        show_cols = [
            c
            for c in [
                "period_ending",
                "total_revenue",
                "cost_of_revenue",
                "gross_profit",
                "operating_income",
                "net_income",
                "ebitda",
                "stock_based_compensation",
                "adjusted_ebitda",
                "diluted_earnings_per_share",
            ]
            if c in df.columns
        ]
        st.dataframe(
            df[show_cols].sort_values("period_ending", ascending=False),
            hide_index=True,
        )


def _render_balance_sheet(symbol, df=None, cur="$"):
    """Render balance sheet trends."""
    st.subheader("Balance Sheet")
    if df is None:
        df = fetch_balance(symbol)
    if df.empty or "period_ending" not in df.columns:
        st.info("No balance sheet data available for this symbol")
        return

    df = df.sort_values("period_ending").copy()
    df["period_ending"] = pd.to_datetime(df["period_ending"])
    labels = df["period_ending"].apply(_period_label)

    # Metrics — 2 rows of 3
    latest = df.iloc[-1]
    r1a, r1b, r1c = st.columns(3)
    r1a.metric("Total Assets", _fmt_large(_get_col(latest, "total_assets"), currency=cur))
    equity = _get_col(latest, "total_equity_non_controlling_interests")
    debt = _get_col(latest, "total_debt")
    if equity and debt and equity != 0:
        r1b.metric("Debt/Equity", f"{debt / equity:.2f}")
    r1c.metric("Cash", _fmt_large(_get_col(latest, "cash_and_cash_equivalents"), currency=cur))
    r2a, _, _ = st.columns(3)
    cur_assets = _get_col(latest, "total_current_assets")
    cur_liab = _get_col(latest, "current_liabilities")
    if cur_assets and cur_liab and cur_liab != 0:
        r2a.metric("Current Ratio", f"{cur_assets / cur_liab:.2f}")

    chart_col1, chart_col2 = st.columns(2)

    # Assets vs Liabilities vs Equity
    with chart_col1:
        fig = go.Figure()
        for col, name, color in [
            ("total_assets", "Assets", "#2196F3"),
            ("total_liabilities_net_minority_interest", "Liabilities", "#ef5350"),
            ("total_equity_non_controlling_interests", "Equity", "#26a69a"),
        ]:
            if col in df.columns:
                fig.add_trace(go.Bar(x=labels, y=df[col], name=name, marker_color=color))
        fig.update_layout(
            title="Assets / Liabilities / Equity",
            barmode="group",
            height=350,
            margin=dict(l=40, r=20, t=40, b=30),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Debt vs Cash
    with chart_col2:
        fig = go.Figure()
        if "total_debt" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=labels,
                    y=df["total_debt"],
                    name="Total Debt",
                    mode="lines+markers",
                    line=dict(color="#ef5350"),
                )
            )
        if "cash_and_cash_equivalents" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=labels,
                    y=df["cash_and_cash_equivalents"],
                    name="Cash",
                    mode="lines+markers",
                    line=dict(color="#26a69a"),
                )
            )
        fig.update_layout(title="Debt vs Cash", height=350, margin=dict(l=40, r=20, t=40, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Balance Sheet Data"):
        show_cols = [
            c
            for c in [
                "period_ending",
                "total_assets",
                "total_liabilities_net_minority_interest",
                "total_equity_non_controlling_interests",
                "total_debt",
                "cash_and_cash_equivalents",
                "total_current_assets",
                "current_liabilities",
            ]
            if c in df.columns
        ]
        st.dataframe(df[show_cols].sort_values("period_ending", ascending=False), hide_index=True)


def _render_cash_flow(symbol, df=None, cur="$"):
    """Render cash flow trends."""
    st.subheader("Cash Flow")
    if df is None:
        df = fetch_cashflow(symbol)
    if df.empty or "period_ending" not in df.columns:
        st.info("No cash flow data available for this symbol")
        return

    df = df.sort_values("period_ending").copy()
    df["period_ending"] = pd.to_datetime(df["period_ending"])
    labels = df["period_ending"].apply(_period_label)

    # Metrics — 2 rows of 3
    latest = df.iloc[-1]

    def _fl(v):
        return _fmt_large(v, currency=cur)

    r1a, r1b, r1c = st.columns(3)
    r1a.metric("Operating CF", _fl(_get_col(latest, "operating_cash_flow")))
    r1b.metric("Free Cash Flow", _fl(_get_col(latest, "free_cash_flow")))
    r1c.metric("Capex", _fl(_get_col(latest, "capital_expenditure")))
    r2a, _, _ = st.columns(3)
    buyback = _get_col(latest, "repurchase_of_capital_stock")
    r2a.metric("Buybacks", _fl(buyback) if buyback else "N/A")

    chart_col1, chart_col2 = st.columns(2)

    # Operating / Investing / Financing CF
    with chart_col1:
        fig = go.Figure()
        for col, name, color in [
            ("operating_cash_flow", "Operating", "#26a69a"),
            ("investing_cash_flow", "Investing", "#FF9800"),
            ("financing_cash_flow", "Financing", "#ef5350"),
        ]:
            if col in df.columns:
                fig.add_trace(go.Bar(x=labels, y=df[col], name=name, marker_color=color))
        fig.update_layout(
            title="Cash Flow Components",
            barmode="group",
            height=350,
            margin=dict(l=40, r=20, t=40, b=30),
        )
        st.plotly_chart(fig, use_container_width=True)

    # FCF trend
    with chart_col2:
        fig = go.Figure()
        if "free_cash_flow" in df.columns:
            colors = ["#26a69a" if v >= 0 else "#ef5350" for v in df["free_cash_flow"]]
            fig.add_trace(go.Bar(x=labels, y=df["free_cash_flow"], name="FCF", marker_color=colors))
        fig.update_layout(title="Free Cash Flow", height=350, margin=dict(l=40, r=20, t=40, b=30))
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Cash Flow Data"):
        show_cols = [
            c
            for c in [
                "period_ending",
                "operating_cash_flow",
                "capital_expenditure",
                "free_cash_flow",
                "investing_cash_flow",
                "financing_cash_flow",
                "common_stock_dividend_paid",
                "repurchase_of_capital_stock",
            ]
            if c in df.columns
        ]
        st.dataframe(df[show_cols].sort_values("period_ending", ascending=False), hide_index=True)


def _render_ratio_analysis(symbol, income_df=None, balance_df=None, cashflow_df=None):
    """Render financial ratio analysis."""
    st.subheader("Ratio Analysis")

    if income_df is None:
        income_df = fetch_income(symbol)
    if balance_df is None:
        balance_df = fetch_balance(symbol)
    if cashflow_df is None:
        cashflow_df = fetch_cashflow(symbol)

    ratios = compute_financial_ratios(income_df, balance_df, cashflow_df)
    if ratios.empty:
        st.info("Insufficient data for ratio analysis")
        return

    labels = ratios["period_ending"].apply(_period_label)

    chart_col1, chart_col2 = st.columns(2)

    # Profitability
    with chart_col1:
        fig = go.Figure()
        for col, name, color in [
            ("gross_margin", "Gross Margin", "#2196F3"),
            ("operating_margin", "Operating Margin", "#FF9800"),
            ("net_margin", "Net Margin", "#26a69a"),
            ("fcf_margin", "FCF Margin", "#9C27B0"),
        ]:
            if col in ratios.columns:
                fig.add_trace(
                    go.Scatter(
                        x=labels,
                        y=(ratios[col] * 100).round(1),
                        name=name,
                        mode="lines+markers",
                        line=dict(color=color),
                    )
                )
        fig.update_layout(
            title="Profitability Ratios (%)",
            height=350,
            margin=dict(l=40, r=20, t=40, b=30),
            yaxis_title="%",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Returns & Leverage
    with chart_col2:
        fig = go.Figure()
        for col, name, color in [
            ("roe", "ROE", "#2196F3"),
            ("roa", "ROA", "#26a69a"),
        ]:
            if col in ratios.columns:
                fig.add_trace(
                    go.Scatter(
                        x=labels,
                        y=(ratios[col] * 100).round(1),
                        name=name,
                        mode="lines+markers",
                        line=dict(color=color),
                    )
                )
        if "current_ratio" in ratios.columns:
            fig.add_trace(
                go.Scatter(
                    x=labels,
                    y=ratios["current_ratio"],
                    name="Current Ratio",
                    mode="lines+markers",
                    line=dict(color="#FF9800", dash="dot"),
                    yaxis="y2",
                )
            )
        fig.update_layout(
            title="Returns & Leverage",
            height=350,
            margin=dict(l=40, r=20, t=40, b=30),
            yaxis_title="%",
            yaxis2=dict(title="Ratio", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Ratio Data"):
        display = ratios.copy()
        display["period_ending"] = labels.values
        # Format as percentages for display
        pct_cols = ["gross_margin", "operating_margin", "net_margin", "roe", "roa", "fcf_margin"]
        for c in pct_cols:
            if c in display.columns:
                display[c] = display[c].apply(lambda v: f"{v * 100:.1f}%" if pd.notna(v) else "N/A")
        st.dataframe(display.sort_index(ascending=False), hide_index=True)


# =========================================================================
# Valuation History tab rendering (Enhancement #1)
# =========================================================================


def _render_valuation_history(symbol, db, cur="$"):
    """Render historical valuation multiples (PE, PB, EV/EBITDA, FCF yield)."""
    inc_df = fetch_income(symbol, "quarter")
    bal_df = fetch_balance(symbol, "quarter")
    cf_df = fetch_cashflow(symbol, "quarter")
    price_df = db.get_price_history_batch([symbol], days=2000)
    if not price_df.empty:
        price_df = price_df[price_df["symbol"] == symbol]

    # Compute FX rate when reporting currency != trading currency
    # (e.g. BABA reports in CNY but trades in USD)
    fin_cur = fetch_reporting_currency(symbol)
    trade_cur = fetch_trading_currency(symbol)
    fx = fetch_fx_rate(fin_cur, trade_cur) if fin_cur != trade_cur else 1.0
    if fx != 1.0:
        st.caption(f"Currency adjustment applied: {fin_cur} → {trade_cur} (rate: {fx:.2f})")

    vals = compute_historical_valuations(inc_df, bal_df, cf_df, price_df, fx_rate=fx)
    if vals.empty:
        st.info("Insufficient data for historical valuation analysis")
        return

    labels = vals["period_ending"].apply(_period_label)

    # Valuation Zone indicator
    latest = vals.iloc[-1]
    zone_items = []
    for metric, label in [("pe", "PE"), ("pb", "PB"), ("ev_ebitda", "EV/EBITDA")]:
        series = vals[metric].dropna()
        if len(series) >= 4:
            current = latest.get(metric)
            if current is not None and not pd.isna(current):
                pct_rank = (series < current).sum() / len(series) * 100
                if pct_rank <= 25:
                    zone_items.append((label, current, "Cheap", "#26a69a"))
                elif pct_rank >= 75:
                    zone_items.append((label, current, "Expensive", "#ef5350"))
                else:
                    zone_items.append((label, current, "Fair", "#FF9800"))

    if zone_items:
        st.subheader("Valuation Zone")
        cols = st.columns(len(zone_items))
        for col, (label, value, zone, color) in zip(cols, zone_items):
            col.metric(label, f"{value:.1f}")
            col.markdown(f":{color.replace('#', '')}[**{zone}**] vs own history")

    st.divider()

    # Chart 1: PE / PB over time with median lines
    chart1, chart2 = st.columns(2)
    with chart1:
        fig = go.Figure()
        if "pe" in vals.columns:
            pe_clean = vals[vals["pe"].notna() & (vals["pe"] > 0) & (vals["pe"] < 200)]
            if not pe_clean.empty:
                pe_labels = pe_clean["period_ending"].apply(_period_label)
                fig.add_trace(
                    go.Scatter(
                        x=pe_labels,
                        y=pe_clean["pe"],
                        name="PE",
                        mode="lines+markers",
                        line=dict(color="#2196F3"),
                    )
                )
                pe_median = pe_clean["pe"].median()
                fig.add_hline(
                    y=pe_median,
                    line_dash="dash",
                    line_color="#2196F3",
                    opacity=0.5,
                    annotation_text=f"PE Median: {pe_median:.1f}",
                )
        if "pb" in vals.columns:
            pb_clean = vals[vals["pb"].notna() & (vals["pb"] > 0) & (vals["pb"] < 100)]
            if not pb_clean.empty:
                pb_labels = pb_clean["period_ending"].apply(_period_label)
                fig.add_trace(
                    go.Scatter(
                        x=pb_labels,
                        y=pb_clean["pb"],
                        name="PB",
                        mode="lines+markers",
                        line=dict(color="#FF9800"),
                        yaxis="y2",
                    )
                )
                pb_median = pb_clean["pb"].median()
                fig.add_hline(
                    y=pb_median,
                    line_dash="dash",
                    line_color="#FF9800",
                    opacity=0.5,
                )
        fig.update_layout(
            title="PE & PB Ratio History",
            height=400,
            margin=dict(l=40, r=40, t=40, b=30),
            yaxis_title="PE",
            yaxis2=dict(title="PB", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Chart 2: EV/EBITDA and FCF Yield
    with chart2:
        fig = go.Figure()
        if "ev_ebitda" in vals.columns:
            ev_clean = vals[vals["ev_ebitda"].notna() & (vals["ev_ebitda"] > 0)]
            if not ev_clean.empty:
                ev_labels = ev_clean["period_ending"].apply(_period_label)
                fig.add_trace(
                    go.Scatter(
                        x=ev_labels,
                        y=ev_clean["ev_ebitda"],
                        name="EV/EBITDA",
                        mode="lines+markers",
                        line=dict(color="#9C27B0"),
                    )
                )
                ev_median = ev_clean["ev_ebitda"].median()
                fig.add_hline(
                    y=ev_median,
                    line_dash="dash",
                    line_color="#9C27B0",
                    opacity=0.5,
                    annotation_text=f"Median: {ev_median:.1f}",
                )
        if "fcf_yield" in vals.columns:
            fcf_clean = vals[vals["fcf_yield"].notna()]
            if not fcf_clean.empty:
                fcf_labels = fcf_clean["period_ending"].apply(_period_label)
                fig.add_trace(
                    go.Scatter(
                        x=fcf_labels,
                        y=fcf_clean["fcf_yield"],
                        name="FCF Yield %",
                        mode="lines+markers",
                        line=dict(color="#26a69a"),
                        yaxis="y2",
                    )
                )
        fig.update_layout(
            title="EV/EBITDA & FCF Yield History",
            height=400,
            margin=dict(l=40, r=40, t=40, b=30),
            yaxis_title="EV/EBITDA",
            yaxis2=dict(title="FCF Yield %", overlaying="y", side="right"),
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Valuation Data"):
        display = vals.copy()
        display["period_ending"] = labels.values
        val_cols = ["period_ending", "close_price", "pe", "pb", "ev_ebitda", "fcf_yield"]
        show_cols = [c for c in val_cols if c in display.columns]
        st.dataframe(display[show_cols].sort_index(ascending=False), hide_index=True)


# =========================================================================
# Earnings & Growth tab rendering (Enhancement #2)
# =========================================================================


def _render_earnings_growth(symbol, cur="$"):
    """Render earnings and revenue growth analysis from quarterly data."""
    inc_df = fetch_income(symbol, "quarter")
    if inc_df.empty or "period_ending" not in inc_df.columns:
        st.info("No quarterly income data available for growth analysis")
        return

    growth = compute_growth_rates(inc_df)
    if growth.empty:
        st.info("Insufficient data for growth analysis")
        return

    labels = growth["period_ending"].apply(_period_label)

    # Summary metrics
    latest = growth.iloc[-1]
    cols = st.columns(4)
    if "rev_yoy" in growth.columns and pd.notna(latest.get("rev_yoy")):
        cols[0].metric("Revenue YoY", f"{latest['rev_yoy']:+.1f}%")
    if "rev_qoq" in growth.columns and pd.notna(latest.get("rev_qoq")):
        cols[1].metric("Revenue QoQ", f"{latest['rev_qoq']:+.1f}%")
    if "eps_yoy" in growth.columns and pd.notna(latest.get("eps_yoy")):
        cols[2].metric("EPS YoY", f"{latest['eps_yoy']:+.1f}%")
    if "rev_accelerating" in growth.columns:
        accel = latest.get("rev_accelerating")
        if pd.notna(accel):
            cols[3].metric("Growth Trend", "Accelerating" if accel else "Decelerating")

    # Chart 1: Revenue growth rate trend
    chart1, chart2 = st.columns(2)
    with chart1:
        fig = go.Figure()
        if "rev_yoy" in growth.columns:
            yoy_clean = growth[growth["rev_yoy"].notna()]
            if not yoy_clean.empty:
                yoy_labels = yoy_clean["period_ending"].apply(_period_label)
                colors = ["#26a69a" if v >= 0 else "#ef5350" for v in yoy_clean["rev_yoy"]]
                fig.add_trace(
                    go.Bar(
                        x=yoy_labels,
                        y=yoy_clean["rev_yoy"],
                        name="YoY Growth",
                        marker_color=colors,
                    )
                )
        if "rev_qoq" in growth.columns:
            qoq_clean = growth[growth["rev_qoq"].notna()]
            if not qoq_clean.empty:
                qoq_labels = qoq_clean["period_ending"].apply(_period_label)
                fig.add_trace(
                    go.Scatter(
                        x=qoq_labels,
                        y=qoq_clean["rev_qoq"],
                        name="QoQ Growth",
                        mode="lines+markers",
                        line=dict(color="#FF9800", dash="dot"),
                    )
                )
        fig.add_hline(y=0, line_dash="solid", line_color="gray", opacity=0.3)
        fig.update_layout(
            title="Revenue Growth Rate (%)",
            height=400,
            margin=dict(l=40, r=20, t=40, b=30),
            yaxis_title="%",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Chart 2: EPS quarterly trend
    with chart2:
        fig = go.Figure()
        if "eps" in growth.columns:
            eps_clean = growth[growth["eps"].notna()]
            if not eps_clean.empty:
                eps_labels = eps_clean["period_ending"].apply(_period_label)
                fig.add_trace(
                    go.Bar(
                        x=eps_labels,
                        y=eps_clean["eps"],
                        name="Quarterly EPS",
                        marker_color="#2196F3",
                    )
                )
                # Add TTM EPS as overlay (rolling 4-quarter sum)
                if len(eps_clean) >= 4:
                    ttm_eps = eps_clean["eps"].rolling(4).sum()
                    fig.add_trace(
                        go.Scatter(
                            x=eps_labels,
                            y=ttm_eps,
                            name="TTM EPS",
                            mode="lines+markers",
                            line=dict(color="#9C27B0", width=2),
                        )
                    )
        fig.update_layout(
            title=f"EPS Trend ({cur})",
            height=400,
            margin=dict(l=40, r=20, t=40, b=30),
            yaxis_title=f"EPS ({cur})",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Revenue bars
    if "revenue" in growth.columns:
        rev_clean = growth[growth["revenue"].notna()]
        if not rev_clean.empty:
            rev_labels = rev_clean["period_ending"].apply(_period_label)
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=rev_labels,
                    y=rev_clean["revenue"],
                    name="Quarterly Revenue",
                    marker_color="#2196F3",
                )
            )
            fig.update_layout(
                title="Quarterly Revenue",
                height=350,
                margin=dict(l=40, r=20, t=40, b=30),
            )
            st.plotly_chart(fig, use_container_width=True)

    with st.expander("Growth Data"):
        display = growth.copy()
        display["period_ending"] = labels.values
        show_cols = [
            c
            for c in ["period_ending", "revenue", "eps", "rev_qoq", "rev_yoy", "eps_qoq", "eps_yoy"]
            if c in display.columns
        ]
        for c in ["rev_qoq", "rev_yoy", "eps_qoq", "eps_yoy"]:
            if c in display.columns:
                display[c] = display[c].apply(lambda v: f"{v:+.1f}%" if pd.notna(v) else "N/A")
        st.dataframe(display[show_cols].sort_index(ascending=False), hide_index=True)


# =========================================================================
# Side-by-Side Comparison tab rendering (Enhancement #3)
# =========================================================================


def _render_comparison(symbols, db, cur="$"):
    """Render side-by-side comparison of 2-3 symbols."""
    if len(symbols) < 2:
        st.info("Select at least 2 symbols to compare")
        return

    # Normalized price performance
    st.subheader("Price Performance (Normalized)")
    price_dfs = {}
    for sym in symbols:
        pdf = db.get_price_history_batch([sym], days=252)
        if not pdf.empty:
            price_dfs[sym] = pdf[pdf["symbol"] == sym]
    normalized = normalize_price_series(price_dfs)
    if not normalized.empty:
        fig = go.Figure()
        colors = ["#2196F3", "#FF9800", "#26a69a"]
        for i, sym in enumerate(normalized.columns):
            fig.add_trace(
                go.Scatter(
                    x=normalized.index,
                    y=normalized[sym],
                    name=sym,
                    mode="lines",
                    line=dict(color=colors[i % len(colors)], width=2),
                )
            )
        fig.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.3)
        fig.update_layout(
            height=400,
            margin=dict(l=40, r=20, t=20, b=30),
            yaxis_title="Indexed (100 = start)",
            xaxis_title="",
        )
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No overlapping price data for comparison")

    st.divider()

    # Key metrics side-by-side
    st.subheader("Key Metrics")
    fundamentals_df = db.get_all_fundamentals()
    metric_rows = []
    for sym in symbols:
        row = {"Symbol": sym}
        if not fundamentals_df.empty:
            sym_fund = fundamentals_df[fundamentals_df["symbol"] == sym]
            if not sym_fund.empty:
                f = sym_fund.iloc[0]
                row["PE"] = _fmt(f.get("pe_ratio"), ".1f")
                row["PB"] = _fmt(f.get("pb_ratio"), ".2f")
                # Market cap from DB is in trading currency (USD for all our symbols)
                row["Market Cap"] = _fmt_large(f.get("market_cap"), currency="$")
                row["D/E"] = _fmt(f.get("debt_to_equity"), ".2f")
                row["Div Yield"] = _fmt(f.get("dividend_yield"), ".2%")
        # Technicals
        prices = db.get_price_history_batch([sym], days=90)
        if not prices.empty:
            tech = compute_price_technicals(prices[prices["symbol"] == sym], sym)
            row["90d Return"] = _fmt(tech.get("total_return_pct"), "+.1f", suffix="%")
            row["Volatility"] = _fmt(tech.get("daily_volatility"), ".4f")
            row["Max DD"] = _fmt(tech.get("max_drawdown_pct"), ".1f", suffix="%")
        metric_rows.append(row)

    if metric_rows:
        st.dataframe(pd.DataFrame(metric_rows), hide_index=True)

    st.divider()

    # Margin comparison
    st.subheader("Margin Comparison")
    margin_fig = go.Figure()
    colors = ["#2196F3", "#FF9800", "#26a69a"]
    for i, sym in enumerate(symbols):
        inc = fetch_income(sym, "quarter")
        if not inc.empty and "period_ending" in inc.columns and "total_revenue" in inc.columns:
            inc = inc.sort_values("period_ending").copy()
            inc["period_ending"] = pd.to_datetime(inc["period_ending"])
            if "operating_income" in inc.columns:
                margin = (inc["operating_income"] / inc["total_revenue"].replace(0, pd.NA)) * 100
                inc_labels = inc["period_ending"].apply(_period_label)
                margin_fig.add_trace(
                    go.Scatter(
                        x=inc_labels,
                        y=margin,
                        name=f"{sym} Op Margin",
                        mode="lines+markers",
                        line=dict(color=colors[i % len(colors)]),
                    )
                )
    margin_fig.update_layout(
        title="Operating Margin Trends (%)",
        height=400,
        margin=dict(l=40, r=20, t=40, b=30),
        yaxis_title="%",
    )
    st.plotly_chart(margin_fig, use_container_width=True)

    st.divider()

    # Valuation comparison
    st.subheader("Valuation Comparison")
    val_rows = []
    for sym in symbols:
        inc = fetch_income(sym, "quarter")
        bal = fetch_balance(sym, "quarter")
        cf = fetch_cashflow(sym, "quarter")
        pdf_sym = db.get_price_history_batch([sym], days=2000)
        if not pdf_sym.empty:
            pdf_sym = pdf_sym[pdf_sym["symbol"] == sym]
        sym_fin_cur = fetch_reporting_currency(sym)
        sym_trade_cur = fetch_trading_currency(sym)
        sym_fx = fetch_fx_rate(sym_fin_cur, sym_trade_cur) if sym_fin_cur != sym_trade_cur else 1.0
        vals = compute_historical_valuations(inc, bal, cf, pdf_sym, fx_rate=sym_fx)
        if not vals.empty:
            latest_v = vals.iloc[-1]
            val_rows.append(
                {
                    "Symbol": sym,
                    "PE": _fmt(latest_v.get("pe"), ".1f"),
                    "PB": _fmt(latest_v.get("pb"), ".2f"),
                    "EV/EBITDA": _fmt(latest_v.get("ev_ebitda"), ".1f"),
                    "FCF Yield %": _fmt(latest_v.get("fcf_yield"), ".2f"),
                }
            )
    if val_rows:
        st.dataframe(pd.DataFrame(val_rows), hide_index=True)


# =========================================================================
# Research Notes rendering (Enhancement #4)
# =========================================================================


def _render_research_notes(symbol, db):
    """Render research notes section with add/view/delete."""
    st.subheader("Research Notes")

    # Input for new note
    new_note = st.text_area(
        "Add a research note / investment thesis",
        key=f"note_input_{symbol}",
        height=100,
        placeholder=f"e.g., Bullish on {symbol} because...",
    )
    if st.button("Save Note", key=f"save_note_{symbol}"):
        if new_note.strip():
            db.save_note(symbol, new_note.strip())
            st.success("Note saved!")
            st.rerun()
        else:
            st.warning("Note cannot be empty")

    # Display existing notes
    notes_df = db.get_notes(symbol)
    if notes_df.empty:
        st.info(f"No research notes for {symbol} yet")
        return

    st.markdown(f"**{len(notes_df)} note(s) on record**")
    for _, row in notes_df.iterrows():
        with st.expander(f"{row['updated_at']}", expanded=(notes_df.index[0] == _)):
            st.markdown(row["note"])
            if st.button("Delete", key=f"del_note_{row['id']}"):
                db.delete_note(row["id"])
                st.rerun()


# =========================================================================
# Insider & Institutional Activity rendering (Enhancement #5)
# =========================================================================


def _render_insider_institutional(symbol):
    """Render insider trading and institutional ownership."""
    # Insider Trading
    st.subheader("Insider Trading Activity")
    insider_df = fetch_insider_trades(symbol)
    if insider_df.empty:
        st.info("No insider trading data available")
    else:
        summary = summarize_insider_activity(insider_df)
        cols = st.columns(4)
        cols[0].metric("Total Trades", summary.get("total_trades", 0))
        cols[1].metric("Buys", summary.get("buys", 0))
        cols[2].metric("Sells", summary.get("sells", 0))
        net = summary.get("net_shares")
        if net is not None:
            cols[3].metric("Net Shares", f"{net:+,}")

        # Recent trades table
        recent = summary.get("recent_trades", [])
        if recent:
            st.markdown("**Recent Trades**")
            st.dataframe(pd.DataFrame(recent), hide_index=True)

        # Top insiders
        top = summary.get("top_insiders", [])
        if top:
            with st.expander("Top Insiders by Trade Count"):
                st.dataframe(pd.DataFrame(top), hide_index=True)

    st.divider()

    # Institutional Ownership
    st.subheader("Institutional Ownership")
    inst_df = fetch_institutional(symbol)
    if inst_df.empty:
        st.info("No institutional ownership data available")
    else:
        # Show top holders
        display_cols = []
        for c in ["investor_name", "shares_held", "percent_of_outstanding", "date_reported"]:
            alt_names = {
                "investor_name": ["investor", "holder_name", "name"],
                "shares_held": ["shares", "value"],
                "percent_of_outstanding": ["percent", "weight", "pct"],
                "date_reported": ["date", "report_date"],
            }
            if c in inst_df.columns:
                display_cols.append(c)
            else:
                for alt in alt_names.get(c, []):
                    if alt in inst_df.columns:
                        display_cols.append(alt)
                        break

        if display_cols:
            st.dataframe(inst_df[display_cols].head(15), hide_index=True)
        else:
            st.dataframe(inst_df.head(15), hide_index=True)


# =========================================================================
# Main page
# =========================================================================


def main():
    st.title("Research")
    st.markdown(
        "**Symbol deep-dive analysis** | Technicals, fundamentals, "
        "valuations, growth, comparison, ownership"
    )

    # Responsive CSS for metric cards on narrow screens
    st.markdown(
        """<style>
        @media (max-width: 768px) {
            [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
            [data-testid="stMetricLabel"] { font-size: 0.75rem !important; }
            [data-testid="column"] { min-width: 0 !important; padding: 0 4px !important; }
        }
        @media (max-width: 480px) {
            [data-testid="stMetricValue"] { font-size: 0.95rem !important; }
            [data-testid="stMetricLabel"] { font-size: 0.7rem !important; }
            .stTabs [data-baseweb="tab"] { font-size: 0.85rem !important; }
        }
        </style>""",
        unsafe_allow_html=True,
    )

    render_sidebar_controls()

    db = get_db()

    # Symbol selector (defaults to the symbol selected on the dashboard)
    default_sym = st.session_state.get("selected_symbol", ALL_SYMBOLS[0])
    default_idx = ALL_SYMBOLS.index(default_sym) if default_sym in ALL_SYMBOLS else 0
    symbol = st.selectbox("Select Symbol", ALL_SYMBOLS, index=default_idx)
    if not symbol:
        return

    # Comparison mode toggle
    compare_mode = st.checkbox("Compare symbols side-by-side")
    compare_symbols = []
    if compare_mode:
        compare_symbols = st.multiselect(
            "Select symbols to compare (2-3)",
            ALL_SYMBOLS,
            default=[symbol],
            max_selections=3,
        )

    result = run_deep_analysis(symbol, db)
    deep = result["deep"]
    peer_ctx = result["peer_context"]
    category = result["category"]
    peers = result["peers"]

    # Reporting currency (shared across tabs)
    _CURRENCY_SYMBOLS = {
        "USD": "$",
        "CNY": "\u00a5",
        "TWD": "NT$",
        "JPY": "\u00a5",
        "EUR": "\u20ac",
        "GBP": "\u00a3",
        "HKD": "HK$",
        "KRW": "\u20a9",
    }
    fin_currency = fetch_reporting_currency(symbol)
    cur = _CURRENCY_SYMBOLS.get(fin_currency, fin_currency + " ")

    # =====================================================================
    # TABS
    # =====================================================================
    if compare_mode and len(compare_symbols) >= 2:
        tab_compare, tab_overview, tab_financials, tab_valuation, tab_growth, tab_ownership = (
            st.tabs(
                [
                    "Comparison",
                    "Overview",
                    "Financial Statements",
                    "Valuation History",
                    "Earnings & Growth",
                    "Insider & Institutional",
                ]
            )
        )
        with tab_compare:
            _render_comparison(compare_symbols, db, cur)
    else:
        tab_overview, tab_financials, tab_valuation, tab_growth, tab_ownership = st.tabs(
            [
                "Overview",
                "Financial Statements",
                "Valuation History",
                "Earnings & Growth",
                "Insider & Institutional",
            ]
        )

    # =====================================================================
    # TAB: Overview
    # =====================================================================
    with tab_overview:
        # Section 1: Signals
        signals = deep.get("signals", [])
        if signals:
            st.subheader("Signals")
            for sig in signals:
                if (
                    "weakness" in sig.lower()
                    or "drawdown" in sig.lower()
                    or "high pe" in sig.lower()
                ):
                    st.warning(sig)
                elif "momentum" in sig.lower() or "undervalued" in sig.lower():
                    st.success(sig)
                elif "surge" in sig.lower():
                    st.info(sig)
                else:
                    st.info(sig)

        st.divider()

        # Section 2: Technical Summary
        st.subheader("Technical Summary")
        tech = deep.get("technical_summary", {})
        if tech.get("error"):
            st.warning(f"No technical data: {tech['error']}")
        else:
            col_metrics, col_chart = st.columns(2)
            with col_metrics:
                st.markdown("**Key Metrics**")
                trend = tech.get("trend", "unknown")
                trend_display = (
                    "Above SMA-20"
                    if trend == "above"
                    else ("Below SMA-20" if trend == "below" else "Unknown")
                )
                st.metric("SMA-20 Position", trend_display)
                st.metric("Total Return", _fmt(tech.get("total_return_pct"), "+.1f", suffix="%"))
                st.metric("Max Drawdown", _fmt(tech.get("max_drawdown_pct"), ".1f", suffix="%"))
                st.metric("Daily Volatility", _fmt(tech.get("daily_volatility"), ".4f"))

            with col_chart:
                st.markdown("**90-Day Price Chart**")
                history = db.get_latest_prices(symbol, days=90)
                if not history.empty and "date" in history.columns:
                    history["date"] = pd.to_datetime(history["date"])
                    history = history.sort_values("date")
                    fig = go.Figure(
                        go.Candlestick(
                            x=history["date"],
                            open=history["open"],
                            high=history["high"],
                            low=history["low"],
                            close=history["close"],
                            increasing_line_color="#26a69a",
                            decreasing_line_color="#ef5350",
                        )
                    )
                    fig.update_layout(
                        height=350,
                        xaxis_rangeslider_visible=False,
                        margin=dict(l=40, r=20, t=10, b=30),
                        showlegend=False,
                    )
                    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption(f"[Open full chart view →](/Charts?symbol={symbol})")
                else:
                    st.warning("No price history available")

        st.divider()

        # Section 3: Fundamental Summary
        st.subheader("Fundamental Summary")
        fund = deep.get("fundamental_summary", {})

        # Fill gaps from income/cash flow statements when DB fundamentals are incomplete.
        # Note: income/cashflow values are in *reporting* currency (e.g. CNY for BABA),
        # while DB fundamentals (pe, eps, market_cap) are in *trading* currency (USD).
        eps_from_statement = False
        if fund:
            if fund.get("revenue") is None or fund.get("free_cash_flow") is None:
                inc_df = fetch_income(symbol)
                if not inc_df.empty and "period_ending" in inc_df.columns:
                    latest_inc = inc_df.sort_values("period_ending").iloc[-1]
                    if fund.get("revenue") is None and "total_revenue" in inc_df.columns:
                        fund["revenue"] = _get_col(latest_inc, "total_revenue")
                    if fund.get("eps") is None and "diluted_earnings_per_share" in inc_df.columns:
                        fund["eps"] = _get_col(latest_inc, "diluted_earnings_per_share")
                        eps_from_statement = True
            if fund.get("free_cash_flow") is None:
                cf_df = fetch_cashflow(symbol)
                if not cf_df.empty and "period_ending" in cf_df.columns:
                    latest_cf = cf_df.sort_values("period_ending").iloc[-1]
                    if "free_cash_flow" in cf_df.columns:
                        fund["free_cash_flow"] = _get_col(latest_cf, "free_cash_flow")
            if fund.get("dividend_yield") is None or fund.get("pb_ratio") is None:
                try:
                    import yfinance as yf

                    info = yf.Ticker(symbol).info
                    if fund.get("dividend_yield") is None:
                        # Use dividendYield (a percentage, e.g. 0.78 = 0.78%)
                        # NOT trailingAnnualDividendYield which divides
                        # foreign-currency dividends by USD price for ADRs
                        dy = info.get("dividendYield")
                        if dy is not None:
                            fund["dividend_yield"] = dy / 100
                    if fund.get("pb_ratio") is None:
                        pb = info.get("priceToBook")
                        if pb is not None:
                            fund["pb_ratio"] = pb
                except Exception:
                    pass

        if not fund or all(v is None for v in fund.values()):
            st.info("No fundamental data available for this symbol")
        else:
            # Market cap and DB EPS are in trading currency (USD for ADRs);
            # revenue/FCF from income/cash flow statements are in reporting currency.
            trade_cur_sym = _CURRENCY_SYMBOLS.get(fetch_trading_currency(symbol), "$")
            if fin_currency != "USD":
                st.caption(
                    f"Revenue & FCF in {fin_currency} | "
                    f"Market cap & EPS in {fetch_trading_currency(symbol)}"
                )
            f_cols = st.columns(4)
            metrics = [
                ("PE Ratio", fund.get("pe_ratio"), ".1f", "", ""),
                ("PB Ratio", fund.get("pb_ratio"), ".2f", "", ""),
                ("Market Cap", fund.get("market_cap"), None, trade_cur_sym, ""),
                ("EPS", fund.get("eps"), ".2f", cur if eps_from_statement else trade_cur_sym, ""),
                ("Revenue", fund.get("revenue"), None, cur, ""),
                ("Debt/Equity", fund.get("debt_to_equity"), ".2f", "", ""),
                ("Dividend Yield", fund.get("dividend_yield"), ".2%", "", ""),
                ("Free Cash Flow", fund.get("free_cash_flow"), None, cur, ""),
            ]
            for i, (label, value, fmt_str, prefix, suffix) in enumerate(metrics):
                with f_cols[i % 4]:
                    if fmt_str is None:
                        st.metric(label, _fmt_large(value, currency=prefix or "$"))
                    elif fmt_str == ".2%":
                        display = _fmt(value, ".2%") if value is not None else "N/A"
                        st.metric(label, display)
                    else:
                        st.metric(label, _fmt(value, fmt_str, prefix=prefix, suffix=suffix))

        st.divider()

        # Section 4: Peer Comparison
        st.subheader(f"Peer Comparison — {category.title() if category else 'N/A'}")
        if category and len(peers) > 1:
            rankings = peer_ctx.get("rankings", {})
            rank_cols = st.columns(3)
            for col, (label, key) in zip(
                rank_cols,
                [
                    ("By Return", "by_return"),
                    ("By Volatility (lowest first)", "by_volatility_asc"),
                    ("By PE (lowest first)", "by_pe_asc"),
                ],
            ):
                with col:
                    st.markdown(f"**{label}**")
                    ranked = rankings.get(key, [])
                    if ranked:
                        for rank, sym in enumerate(ranked, 1):
                            marker = " **<-**" if sym == symbol else ""
                            st.text(f"{rank}. {sym}{marker}")
                    else:
                        st.text("No data")

            peer_tech = peer_ctx.get("technicals", [])
            if peer_tech:
                st.markdown("**Peer Technicals**")
                tech_df = pd.DataFrame(peer_tech)
                rename = {
                    "symbol": "Symbol",
                    "total_return_pct": "Return %",
                    "daily_volatility": "Volatility",
                    "max_drawdown_pct": "Max DD %",
                    "price_vs_sma20": "vs SMA-20",
                    "volume_trend_ratio": "Vol Trend",
                }
                tech_df = tech_df.rename(columns=rename)
                st.dataframe(tech_df, hide_index=True)
        else:
            st.info("No peer group found for this symbol")

        st.divider()

        # Section 5: SEC Filing Summary
        st.subheader("SEC Filing Summary")
        sec = deep.get("sec_summary", {})
        total = sec.get("total_filings", 0)
        if total == 0:
            st.info("No SEC filings on record")
        else:
            st.metric("Total Filings", total)
            by_type = sec.get("by_type", {})
            if by_type:
                st.markdown("**Filings by Type**")
                type_df = pd.DataFrame(
                    [{"Type": k, "Count": v} for k, v in sorted(by_type.items())]
                )
                st.dataframe(type_df, hide_index=True)
            most_recent = sec.get("most_recent")
            if most_recent:
                st.markdown("**Most Recent Filing**")
                mr_cols = st.columns(3)
                mr_cols[0].metric("Type", most_recent.get("report_type", "N/A"))
                mr_cols[1].metric("Date", str(most_recent.get("filing_date", "N/A")))
                url = most_recent.get("report_url")
                if url:
                    mr_cols[2].markdown(f"[View Filing]({url})")

        st.divider()

        # Section 6: Macro Risk Context
        st.subheader("Macro Risk Context")
        macro_risk = result.get("macro_risk")
        if macro_risk:
            risk_level = macro_risk.get("overall_risk_level", "unknown")
            risk_colors = {"low": "normal", "moderate": "off", "elevated": "off", "high": "inverse"}
            st.metric(
                "Overall Risk Level",
                risk_level.upper(),
                delta_color=risk_colors.get(risk_level, "normal"),
            )

            macro_env = macro_risk.get("macro_environment", {})
            env_cols = st.columns(3)
            env_cols[0].metric("Yield Curve", (macro_env.get("yield_curve") or "N/A").title())
            env_cols[1].metric("VIX Regime", (macro_env.get("vix_regime") or "N/A").title())
            env_cols[2].metric("Rate Direction", (macro_env.get("rate_direction") or "N/A").title())

            concerns = macro_risk.get("concerns", [])
            for concern in concerns:
                factor = concern["factor"].replace("_", " ").title()
                msg = f"**{factor}** ({concern['status']}): {concern['impact']}"
                st.warning(msg)
        else:
            st.info("No macro data available — run the economic pipeline to populate")

        st.divider()

        # Section 7: Opportunity Score
        st.subheader("Opportunity Score")
        opportunities = result.get("opportunities", [])
        sym_opp = [o for o in opportunities if o.get("symbol") == symbol]
        if sym_opp:
            opp = sym_opp[0]
            st.metric("Score", f"{opp['score']}/10")
            for reason in opp.get("reasons", []):
                st.success(reason)
        else:
            other_opps = opportunities[:3]
            if other_opps:
                st.info(f"No opportunity signal for {symbol} at current criteria")
                st.markdown("**Top opportunities in watchlist:**")
                for o in other_opps:
                    st.text(f"  {o['symbol']} (score {o['score']}): {', '.join(o['reasons'])}")
            else:
                st.info("No opportunities identified across watchlist")

        st.divider()

        # Section 8: Research Notes
        _render_research_notes(symbol, db)

    # =====================================================================
    # TAB: Financial Statements
    # =====================================================================
    with tab_financials:
        view_mode = st.radio(
            "View",
            ["Annual (Fiscal Year)", "Annual (Trailing 4Q)", "Quarterly"],
            horizontal=True,
        )

        # Determine period and whether to compute TTM
        is_quarterly_view = view_mode == "Quarterly"
        is_ttm = view_mode == "Annual (Trailing 4Q)"
        api_period = "quarter" if (is_quarterly_view or is_ttm) else "annual"

        # Flow-statement columns to sum for TTM
        _INCOME_SUM_COLS = [
            "total_revenue",
            "cost_of_revenue",
            "gross_profit",
            "operating_income",
            "net_income",
            "ebitda",
            "operating_expense",
            "total_pre_tax_income",
            "tax_provision",
        ]
        _CF_SUM_COLS = [
            "operating_cash_flow",
            "free_cash_flow",
            "capital_expenditure",
            "investing_cash_flow",
            "financing_cash_flow",
            "common_stock_dividend_paid",
            "repurchase_of_capital_stock",
            "stock_based_compensation",
        ]

        inc_df = fetch_income(symbol, api_period)
        bal_df = fetch_balance(symbol, api_period)
        cf_df = fetch_cashflow(symbol, api_period)

        if is_ttm:
            inc_df = compute_ttm(inc_df, _INCOME_SUM_COLS)
            cf_df = compute_ttm(cf_df, _CF_SUM_COLS)
            # Balance sheet: use point-in-time snapshot (no summing)
            if not bal_df.empty and "period_ending" in bal_df.columns:
                bal_df = bal_df.sort_values("period_ending")
                # Keep only quarters that have 4 trailing quarters for income
                if not inc_df.empty:
                    ttm_dates = set(inc_df["period_ending"].dt.strftime("%Y-%m-%d"))
                    bal_df = bal_df[bal_df["period_ending"].astype(str).isin(ttm_dates)]

        if fin_currency != "USD":
            st.info(f"Financial data reported in **{fin_currency}** (reporting currency)")

        _render_income_statement(symbol, inc_df, cf_df, cur)
        st.divider()
        _render_balance_sheet(symbol, bal_df, cur)
        st.divider()
        _render_cash_flow(symbol, cf_df, cur)
        st.divider()
        _render_ratio_analysis(symbol, inc_df, bal_df, cf_df)

    # =====================================================================
    # TAB: Valuation History (Enhancement #1)
    # =====================================================================
    with tab_valuation:
        _render_valuation_history(symbol, db, cur)

    # =====================================================================
    # TAB: Earnings & Growth (Enhancement #2)
    # =====================================================================
    with tab_growth:
        _render_earnings_growth(symbol, cur)

    # =====================================================================
    # TAB: Insider & Institutional (Enhancement #5)
    # =====================================================================
    with tab_ownership:
        _render_insider_institutional(symbol)

    # =====================================================================
    # FOOTER
    # =====================================================================
    st.divider()
    st.caption(
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data source: OpenBB (Yahoo Finance, SEC EDGAR)"
    )


main()
