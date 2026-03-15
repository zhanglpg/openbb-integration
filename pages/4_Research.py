"""
Research Dashboard Page
Deep-dive analysis for individual symbols with peer comparison.
"""

import glob  # noqa: I001
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from shared import get_db, render_sidebar_controls  # must be first: adds src/ to sys.path

from analysis import compute_price_technicals, compute_portfolio_risk, compute_valuation_screen
from config import WATCHLIST
from research import analyze_symbol_deep, compare_peers

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

    return {
        "deep": deep,
        "peer_context": peer_context,
        "technicals": technicals,
        "category": category,
        "peers": peers,
    }


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


def _fmt_large(value, fallback="N/A"):
    """Format large numbers (market cap, revenue, FCF) with B/M suffix."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"${value / 1e12:.2f}T"
    if abs_val >= 1e9:
        return f"${value / 1e9:.2f}B"
    if abs_val >= 1e6:
        return f"${value / 1e6:.1f}M"
    return f"${value:,.0f}"


def main():
    st.title("Research")
    st.markdown("**Symbol deep-dive analysis** | Technicals, fundamentals, peers, SEC filings")

    render_sidebar_controls()

    db = get_db()

    # Symbol selector
    symbol = st.selectbox("Select Symbol", ALL_SYMBOLS)
    if not symbol:
        return

    result = run_deep_analysis(symbol, db)
    deep = result["deep"]
    peer_ctx = result["peer_context"]
    category = result["category"]
    peers = result["peers"]

    # =========================================================================
    # Section 1: Signals
    # =========================================================================
    signals = deep.get("signals", [])
    if signals:
        st.subheader("Signals")
        for sig in signals:
            if "weakness" in sig.lower() or "drawdown" in sig.lower() or "high pe" in sig.lower():
                st.warning(sig)
            elif "momentum" in sig.lower() or "undervalued" in sig.lower():
                st.success(sig)
            elif "surge" in sig.lower():
                st.info(sig)
            else:
                st.info(sig)

    st.divider()

    # =========================================================================
    # Section 2: Technical Summary
    # =========================================================================
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
            st.metric(
                "Daily Volatility",
                _fmt(tech.get("daily_volatility"), ".4f"),
            )

        with col_chart:
            st.markdown("**90-Day Price Chart**")
            history = db.get_latest_prices(symbol, days=90)
            if not history.empty and "date" in history.columns:
                history["date"] = pd.to_datetime(history["date"])
                history = history.sort_values("date")
                chart_data = history.set_index("date")[["close"]]
                chart_data.columns = ["Close"]
                st.line_chart(chart_data)
            else:
                st.warning("No price history available")

    st.divider()

    # =========================================================================
    # Section 3: Fundamental Summary
    # =========================================================================
    st.subheader("Fundamental Summary")

    fund = deep.get("fundamental_summary", {})
    if not fund or all(v is None for v in fund.values()):
        st.info("No fundamental data available for this symbol")
    else:
        cols = st.columns(4)
        metrics = [
            ("PE Ratio", fund.get("pe_ratio"), ".1f", "", ""),
            ("PB Ratio", fund.get("pb_ratio"), ".2f", "", ""),
            ("Market Cap", fund.get("market_cap"), None, "", ""),
            ("EPS", fund.get("eps"), ".2f", "$", ""),
            ("Revenue", fund.get("revenue"), None, "", ""),
            ("Debt/Equity", fund.get("debt_to_equity"), ".2f", "", ""),
            ("Dividend Yield", fund.get("dividend_yield"), ".2%", "", ""),
            ("Free Cash Flow", fund.get("free_cash_flow"), None, "", ""),
        ]
        for i, (label, value, fmt, prefix, suffix) in enumerate(metrics):
            with cols[i % 4]:
                if fmt is None:
                    st.metric(label, _fmt_large(value))
                elif fmt == ".2%":
                    display = _fmt(value, ".2%") if value is not None else "N/A"
                    st.metric(label, display)
                else:
                    st.metric(label, _fmt(value, fmt, prefix=prefix, suffix=suffix))

    st.divider()

    # =========================================================================
    # Section 4: Peer Comparison
    # =========================================================================
    st.subheader(f"Peer Comparison — {category.title() if category else 'N/A'}")

    if category and len(peers) > 1:
        # Rankings
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

        # Peer technicals table
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

    # =========================================================================
    # Section 5: SEC Filing Summary
    # =========================================================================
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
            type_df = pd.DataFrame([{"Type": k, "Count": v} for k, v in sorted(by_type.items())])
            st.dataframe(type_df, hide_index=True)

        most_recent = sec.get("most_recent")
        if most_recent:
            st.markdown("**Most Recent Filing**")
            cols = st.columns(3)
            cols[0].metric("Type", most_recent.get("report_type", "N/A"))
            cols[1].metric("Date", str(most_recent.get("filing_date", "N/A")))
            url = most_recent.get("report_url")
            if url:
                cols[2].markdown(f"[View Filing]({url})")

    # =========================================================================
    # FOOTER
    # =========================================================================
    st.divider()
    st.caption(
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data source: OpenBB (Yahoo Finance, SEC EDGAR)"
    )


main()
