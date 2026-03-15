"""Pure analysis functions for MCP tools.

Each function takes DataFrames in and returns dicts/lists out.
No DB access, no OpenBB imports.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def compute_price_technicals(df: pd.DataFrame, symbol: str) -> dict:
    """Compute technical indicators for a single symbol's OHLCV data.

    Args:
        df: DataFrame with columns [date, open, high, low, close, volume].
            Must be sorted by date ascending.
        symbol: The ticker symbol.

    Returns:
        Dict with technical metrics.
    """
    if df.empty or "close" not in df.columns:
        return {"symbol": symbol, "error": "insufficient data"}

    df = df.sort_values("date").reset_index(drop=True)
    closes = df["close"].astype(float)
    n = len(closes)

    latest_close = closes.iloc[-1]

    # Simple moving averages
    sma_5 = round(closes.tail(5).mean(), 2) if n >= 5 else None
    sma_10 = round(closes.tail(10).mean(), 2) if n >= 10 else None
    sma_20 = round(closes.tail(20).mean(), 2) if n >= 20 else None

    price_vs_sma20 = None
    if sma_20 is not None:
        price_vs_sma20 = "above" if latest_close >= sma_20 else "below"

    # Daily returns and volatility
    daily_returns = closes.pct_change().dropna()
    daily_volatility = round(float(daily_returns.std()), 6) if len(daily_returns) > 1 else None

    # Max drawdown
    cummax = closes.cummax()
    drawdown = (closes - cummax) / cummax
    max_drawdown_pct = round(float(drawdown.min()) * 100, 2)

    # Volume metrics
    avg_volume = None
    volume_trend_ratio = None
    if "volume" in df.columns:
        volumes = df["volume"].astype(float)
        avg_volume = int(volumes.mean())
        if n >= 20:
            vol_5d = volumes.tail(5).mean()
            vol_20d = volumes.tail(20).mean()
            volume_trend_ratio = round(vol_5d / vol_20d, 2) if vol_20d > 0 else None

    # High-low range
    high_low_range_pct = None
    if "high" in df.columns and "low" in df.columns:
        period_high = df["high"].astype(float).max()
        period_low = df["low"].astype(float).min()
        if period_low > 0:
            high_low_range_pct = round((period_high - period_low) / period_low * 100, 2)

    # Total return
    total_return_pct = round((latest_close / closes.iloc[0] - 1) * 100, 2)

    return {
        "symbol": symbol,
        "latest_close": latest_close,
        "sma_5": sma_5,
        "sma_10": sma_10,
        "sma_20": sma_20,
        "price_vs_sma20": price_vs_sma20,
        "daily_volatility": daily_volatility,
        "max_drawdown_pct": max_drawdown_pct,
        "avg_volume": avg_volume,
        "volume_trend_ratio": volume_trend_ratio,
        "high_low_range_pct": high_low_range_pct,
        "total_return_pct": total_return_pct,
    }


def compute_valuation_screen(
    fundamentals_df: pd.DataFrame, sort_by: str = "pe_ratio"
) -> list[dict]:
    """Screen symbols by valuation metrics.

    Args:
        fundamentals_df: DataFrame with fundamentals for all symbols.
        sort_by: Column name to sort by (ascending).

    Returns:
        List of dicts with valuation metrics, sorted by sort_by.
    """
    if fundamentals_df.empty:
        return []

    df = fundamentals_df.copy()

    # Compute derived metrics
    if "free_cash_flow" in df.columns and "market_cap" in df.columns:
        df["fcf_yield"] = df.apply(
            lambda r: round(r["free_cash_flow"] / r["market_cap"], 4)
            if pd.notna(r.get("free_cash_flow"))
            and pd.notna(r.get("market_cap"))
            and r["market_cap"] != 0
            else None,
            axis=1,
        )
    else:
        df["fcf_yield"] = None

    if "pe_ratio" in df.columns:
        df["earnings_yield"] = df["pe_ratio"].apply(
            lambda pe: round(1 / pe, 4) if pd.notna(pe) and pe != 0 else None
        )
    else:
        df["earnings_yield"] = None

    # Sort — put NaN at end
    if sort_by in df.columns:
        df = df.sort_values(sort_by, na_position="last")

    # Drop internal columns
    drop_cols = {"id", "fetched_at", "extra_data"}
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    return df.to_dict(orient="records")


def compute_portfolio_risk(prices_df: pd.DataFrame, watchlist: dict) -> dict:
    """Compute portfolio-level risk metrics from multi-symbol price data.

    Args:
        prices_df: DataFrame with columns [symbol, date, close] for multiple symbols.
        watchlist: Dict of category → symbol lists for sector mapping.

    Returns:
        Dict with per-symbol risk, portfolio-level correlation, and sector concentration.
    """
    if prices_df.empty or "close" not in prices_df.columns:
        return {"per_symbol": [], "portfolio": {}, "most_volatile_3": [], "least_volatile_3": []}

    # Build sector mapping
    symbol_sector = {}
    for category, symbols in watchlist.items():
        for s in symbols:
            if s not in symbol_sector:
                symbol_sector[s] = category

    # Pivot to get daily returns per symbol
    pivot = prices_df.pivot_table(index="date", columns="symbol", values="close")
    returns = pivot.pct_change().dropna()

    per_symbol = []
    for sym in returns.columns:
        r = returns[sym].dropna()
        if len(r) < 2:
            continue
        mean_ret = round(float(r.mean()), 6)
        vol = round(float(r.std()), 6)
        # Sharpe proxy: annualized return / annualized vol (assuming 252 trading days)
        sharpe = round((mean_ret * 252) / (vol * np.sqrt(252)), 2) if vol > 0 else None
        per_symbol.append(
            {
                "symbol": sym,
                "mean_daily_return": mean_ret,
                "daily_volatility": vol,
                "sharpe_proxy": sharpe,
                "sector": symbol_sector.get(sym, "unknown"),
            }
        )

    # Sort by volatility for most/least volatile
    per_symbol.sort(key=lambda x: x["daily_volatility"])
    least_volatile_3 = [s["symbol"] for s in per_symbol[:3]]
    most_volatile_3 = [s["symbol"] for s in per_symbol[-3:]][::-1]

    # Portfolio-level correlation
    avg_corr = None
    if len(returns.columns) >= 2:
        corr_matrix = returns.corr()
        # Extract upper triangle (excluding diagonal)
        mask = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        upper_vals = corr_matrix.where(mask).stack()
        if len(upper_vals) > 0:
            avg_corr = round(float(upper_vals.mean()), 3)

    # Sector concentration
    sector_counts: dict[str, int] = {}
    all_symbols = [s["symbol"] for s in per_symbol]
    for sym in all_symbols:
        sec = symbol_sector.get(sym, "unknown")
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
    total = len(all_symbols) or 1
    sector_concentration = {
        sec: round(count / total * 100, 1) for sec, count in sector_counts.items()
    }

    return {
        "per_symbol": per_symbol,
        "portfolio": {
            "avg_pairwise_correlation": avg_corr,
            "sector_concentration": sector_concentration,
        },
        "most_volatile_3": most_volatile_3,
        "least_volatile_3": least_volatile_3,
    }


def compute_macro_snapshot(indicator_histories: dict[str, pd.DataFrame]) -> dict:
    """Compute macro trend analysis from economic indicator time series.

    Args:
        indicator_histories: Dict mapping series_id → DataFrame with [date, value].

    Returns:
        Dict with per-indicator trends and regime assessments.
    """
    if not indicator_histories:
        return {
            "indicators": [],
            "yield_curve_status": None,
            "vix_regime": None,
            "rate_direction": None,
        }

    indicators = []
    for series_id, df in indicator_histories.items():
        if df.empty or "value" not in df.columns:
            continue
        df = df.sort_values("date").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])
        latest = df.iloc[-1]
        latest_val = float(latest["value"])
        latest_date = latest["date"]

        def _change_over(months):
            cutoff = latest_date - timedelta(days=months * 30)
            past = df[df["date"] <= cutoff]
            if past.empty:
                return None
            return round(latest_val - float(past.iloc[-1]["value"]), 4)

        indicators.append(
            {
                "series_id": series_id,
                "latest_value": latest_val,
                "latest_date": latest_date.strftime("%Y-%m-%d"),
                "change_1m": _change_over(1),
                "change_3m": _change_over(3),
                "change_6m": _change_over(6),
                "change_1y": _change_over(12),
            }
        )

    # Yield curve status (T10Y2Y spread)
    yield_curve_status = None
    t10y2y = indicator_histories.get("T10Y2Y")
    if t10y2y is not None and not t10y2y.empty and "value" in t10y2y.columns:
        spread = float(t10y2y.sort_values("date").iloc[-1]["value"])
        if spread < -0.1:
            yield_curve_status = "inverted"
        elif spread > 0.1:
            yield_curve_status = "normal"
        else:
            yield_curve_status = "flat"

    # VIX regime
    vix_regime = None
    vix = indicator_histories.get("VIXCLS")
    if vix is not None and not vix.empty and "value" in vix.columns:
        vix_val = float(vix.sort_values("date").iloc[-1]["value"])
        if vix_val < 15:
            vix_regime = "low"
        elif vix_val < 25:
            vix_regime = "medium"
        else:
            vix_regime = "high"

    # Rate direction (FEDFUNDS)
    rate_direction = None
    ff = indicator_histories.get("FEDFUNDS")
    if ff is not None and not ff.empty and "value" in ff.columns:
        ff_sorted = ff.sort_values("date")
        if len(ff_sorted) >= 2:
            recent = float(ff_sorted.iloc[-1]["value"])
            prev = float(ff_sorted.iloc[-2]["value"])
            diff = recent - prev
            if diff > 0.1:
                rate_direction = "rising"
            elif diff < -0.1:
                rate_direction = "falling"
            else:
                rate_direction = "stable"

    return {
        "indicators": indicators,
        "yield_curve_status": yield_curve_status,
        "vix_regime": vix_regime,
        "rate_direction": rate_direction,
    }


def compute_sec_activity(filings_df: pd.DataFrame, days: int = 90) -> dict:
    """Analyze SEC filing activity across symbols.

    Args:
        filings_df: DataFrame with columns [symbol, filing_date, report_type, ...].
        days: Look back window in days.

    Returns:
        Dict with per-symbol filing activity and recent 8-K highlights.
    """
    if filings_df.empty or "filing_date" not in filings_df.columns:
        return {"per_symbol": [], "recent_8k_activity": [], "inactive_symbols": []}

    df = filings_df.copy()
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    cutoff = datetime.now() - timedelta(days=days)

    # Filter to window
    df_window = df[df["filing_date"] >= cutoff]

    per_symbol = []
    all_symbols = df["symbol"].unique()
    active_symbols = set()

    for sym in all_symbols:
        sym_df = df_window[df_window["symbol"] == sym]
        if sym_df.empty:
            continue
        active_symbols.add(sym)
        by_type = sym_df["report_type"].value_counts().to_dict()
        most_recent = sym_df.sort_values("filing_date", ascending=False).iloc[0]
        days_since = (datetime.now() - most_recent["filing_date"]).days

        per_symbol.append(
            {
                "symbol": sym,
                "total_filings": len(sym_df),
                "by_type": by_type,
                "most_recent_date": most_recent["filing_date"].strftime("%Y-%m-%d"),
                "most_recent_type": most_recent["report_type"],
                "days_since_last": days_since,
            }
        )

    # Recent 8-K activity
    recent_8k = df_window[df_window["report_type"] == "8-K"].sort_values(
        "filing_date", ascending=False
    )
    recent_8k_activity = []
    for _, row in recent_8k.iterrows():
        entry = {
            "symbol": row["symbol"],
            "date": row["filing_date"].strftime("%Y-%m-%d"),
        }
        if "primary_doc_description" in row.index and pd.notna(row["primary_doc_description"]):
            entry["description"] = row["primary_doc_description"]
        recent_8k_activity.append(entry)

    # Inactive symbols
    inactive_symbols = sorted(set(all_symbols) - active_symbols)

    return {
        "per_symbol": per_symbol,
        "recent_8k_activity": recent_8k_activity,
        "inactive_symbols": inactive_symbols,
    }
