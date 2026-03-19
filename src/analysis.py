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


def compute_ttm(quarterly_df: pd.DataFrame, sum_cols: list[str] | None = None) -> pd.DataFrame:
    """Compute trailing twelve months (TTM) from quarterly data.

    For each quarter, sums the most recent 4 quarters of flow-statement
    columns (revenue, income, cash flow, etc.).  The result represents
    annual figures as-of each quarter end.

    Args:
        quarterly_df: DataFrame with ``period_ending`` and numeric columns,
            sorted by period_ending ascending.  Must have at least 4 rows.
        sum_cols: Columns to sum over 4-quarter windows.  If *None*, all
            numeric columns are summed.

    Returns:
        DataFrame with same columns, each row being a 4-quarter trailing sum.
        Rows with fewer than 4 trailing quarters are dropped.
    """
    if quarterly_df is None or quarterly_df.empty:
        return pd.DataFrame()

    df = quarterly_df.copy()
    if "period_ending" not in df.columns:
        return pd.DataFrame()

    df["period_ending"] = pd.to_datetime(df["period_ending"])
    df = df.sort_values("period_ending").reset_index(drop=True)

    if len(df) < 4:
        return pd.DataFrame()

    if sum_cols is None:
        sum_cols = df.select_dtypes(include="number").columns.tolist()

    rows = []
    for i in range(3, len(df)):
        window = df.iloc[i - 3 : i + 1]
        row = {"period_ending": df["period_ending"].iloc[i]}
        for col in sum_cols:
            if col in df.columns:
                row[col] = window[col].sum()
        rows.append(row)

    return pd.DataFrame(rows)


def compute_bollinger_bands(
    df: pd.DataFrame, window: int = 20, num_std: float = 2.0
) -> pd.DataFrame:
    """Compute Bollinger Bands for OHLCV data.

    Args:
        df: DataFrame with 'close' and 'date' columns, sorted by date ascending.
        window: SMA window (default 20).
        num_std: Number of standard deviations (default 2.0).

    Returns:
        DataFrame with columns [date, bb_middle, bb_upper, bb_lower].
    """
    closes = df["close"].astype(float)
    bb_middle = closes.rolling(window=window).mean()
    bb_std = closes.rolling(window=window).std()
    return pd.DataFrame(
        {
            "date": df["date"].values,
            "bb_middle": bb_middle.round(2).values,
            "bb_upper": (bb_middle + num_std * bb_std).round(2).values,
            "bb_lower": (bb_middle - num_std * bb_std).round(2).values,
        }
    )


def compute_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """Compute MACD indicator.

    Args:
        df: DataFrame with 'close' and 'date' columns, sorted by date ascending.
        fast: Fast EMA period (default 12).
        slow: Slow EMA period (default 26).
        signal: Signal line EMA period (default 9).

    Returns:
        DataFrame with columns [date, macd_line, signal_line, histogram].
    """
    closes = df["close"].astype(float)
    ema_fast = closes.ewm(span=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return pd.DataFrame(
        {
            "date": df["date"].values,
            "macd_line": macd_line.round(4).values,
            "signal_line": signal_line.round(4).values,
            "histogram": histogram.round(4).values,
        }
    )


def resample_ohlcv(df: pd.DataFrame, freq: str = "W") -> pd.DataFrame:
    """Resample daily OHLCV data to weekly or monthly.

    Args:
        df: DataFrame with columns [date, open, high, low, close, volume].
        freq: Pandas frequency string — 'W' for weekly, 'ME' for monthly.

    Returns:
        Resampled DataFrame with proper OHLCV aggregation.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    resampled = (
        df.resample(freq)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
    )
    return resampled.reset_index()


def compute_financial_ratios(
    income_df: pd.DataFrame,
    balance_df: pd.DataFrame,
    cashflow_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compute financial ratios from income, balance sheet, and cash flow data.

    Args:
        income_df: Income statement with period_ending, total_revenue, gross_profit,
            operating_income, net_income columns.
        balance_df: Balance sheet with period_ending, total_assets,
            total_liabilities_net_minority_interest,
            total_equity_non_controlling_interests, total_debt,
            total_current_assets, current_liabilities columns.
        cashflow_df: Cash flow with period_ending, free_cash_flow columns.

    Returns:
        DataFrame indexed by period_ending with ratio columns.
        Missing source columns produce NaN ratios.
    """

    def _safe_div(num, den):
        """Element-wise division, returning NaN for zero/missing denominators."""
        return num / den.replace(0, np.nan)

    def _col(df, name):
        """Get column if it exists, else NaN Series aligned to df index."""
        if name in df.columns:
            return df[name].astype(float)
        return pd.Series(np.nan, index=df.index)

    # Align all three statements by period_ending
    dfs = []
    for df, prefix in [(income_df, "inc"), (balance_df, "bal"), (cashflow_df, "cf")]:
        if df is None or df.empty or "period_ending" not in df.columns:
            continue
        d = df.copy()
        d["period_ending"] = pd.to_datetime(d["period_ending"])
        d = d.set_index("period_ending")
        d = d.add_prefix(f"{prefix}_")
        dfs.append(d)

    if not dfs:
        return pd.DataFrame()

    merged = dfs[0]
    for d in dfs[1:]:
        merged = merged.join(d, how="outer")
    merged = merged.sort_index()

    result = pd.DataFrame(index=merged.index)

    # Profitability margins
    rev = _col(merged, "inc_total_revenue")
    result["gross_margin"] = _safe_div(_col(merged, "inc_gross_profit"), rev)
    result["operating_margin"] = _safe_div(_col(merged, "inc_operating_income"), rev)
    result["net_margin"] = _safe_div(_col(merged, "inc_net_income"), rev)

    # Returns
    equity = _col(merged, "bal_total_equity_non_controlling_interests")
    assets = _col(merged, "bal_total_assets")
    net_inc = _col(merged, "inc_net_income")
    result["roe"] = _safe_div(net_inc, equity)
    result["roa"] = _safe_div(net_inc, assets)

    # Leverage
    result["debt_to_equity"] = _safe_div(_col(merged, "bal_total_debt"), equity)
    result["current_ratio"] = _safe_div(
        _col(merged, "bal_total_current_assets"),
        _col(merged, "bal_current_liabilities"),
    )

    # Cash flow
    result["fcf_margin"] = _safe_div(_col(merged, "cf_free_cash_flow"), rev)

    return result.round(4).reset_index()


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
            lambda r: (
                round(r["free_cash_flow"] / r["market_cap"], 4)
                if pd.notna(r.get("free_cash_flow"))
                and pd.notna(r.get("market_cap"))
                and r["market_cap"] != 0
                else None
            ),
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


def compute_historical_valuations(
    income_df: pd.DataFrame,
    balance_df: pd.DataFrame,
    cashflow_df: pd.DataFrame,
    price_df: pd.DataFrame,
    fx_rate: float = 1.0,
) -> pd.DataFrame:
    """Compute historical valuation multiples from quarterly financial data and prices.

    For each quarter-end, finds the closest trading-day close price and computes
    PE, PB, EV/EBITDA, and FCF yield.

    Args:
        income_df: Quarterly income statement with period_ending, net_income,
            ebitda, diluted_earnings_per_share columns.
        balance_df: Quarterly balance sheet with period_ending, total_equity,
            total_debt, cash_and_cash_equivalents columns.
        cashflow_df: Quarterly cash flow with period_ending, free_cash_flow.
        price_df: Daily price history with date, close columns.
        fx_rate: Exchange rate to convert financial data to the price currency.
            E.g. if financials are in CNY and price is in USD, pass the
            CNY-per-USD rate (~7.2) so that financial values are divided by
            this rate before computing multiples.  Default 1.0 (no conversion).

    Returns:
        DataFrame with columns: period_ending, pe, pb, ev_ebitda, fcf_yield,
        plus the underlying components.  Rows sorted by period_ending ascending.
    """
    if income_df is None or income_df.empty:
        return pd.DataFrame()
    if price_df is None or price_df.empty:
        return pd.DataFrame()

    # Prepare price series for lookups
    pdf = price_df.copy()
    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf = pdf.sort_values("date")

    # Prepare income (need TTM for flow metrics)
    inc = income_df.copy()
    if "period_ending" not in inc.columns:
        return pd.DataFrame()
    inc["period_ending"] = pd.to_datetime(inc["period_ending"])
    inc = inc.sort_values("period_ending").reset_index(drop=True)

    # Build TTM income
    ttm_cols = ["net_income", "ebitda", "diluted_earnings_per_share", "total_revenue"]
    ttm_cols = [c for c in ttm_cols if c in inc.columns]
    ttm_income = compute_ttm(inc, ttm_cols) if len(inc) >= 4 else pd.DataFrame()

    # Prepare balance sheet
    bal = pd.DataFrame()
    if balance_df is not None and not balance_df.empty and "period_ending" in balance_df.columns:
        bal = balance_df.copy()
        bal["period_ending"] = pd.to_datetime(bal["period_ending"])
        bal = bal.sort_values("period_ending")

    # Prepare cash flow TTM
    cf_ttm = pd.DataFrame()
    if cashflow_df is not None and not cashflow_df.empty and "period_ending" in cashflow_df.columns:
        cf = cashflow_df.copy()
        cf["period_ending"] = pd.to_datetime(cf["period_ending"])
        cf = cf.sort_values("period_ending").reset_index(drop=True)
        cf_sum_cols = [c for c in ["free_cash_flow"] if c in cf.columns]
        cf_ttm = compute_ttm(cf, cf_sum_cols) if len(cf) >= 4 else pd.DataFrame()

    # Use TTM income dates as the base
    if ttm_income.empty:
        return pd.DataFrame()

    rows = []
    for _, inc_row in ttm_income.iterrows():
        pe_date = inc_row["period_ending"]

        # Find closest price (within 10 days after quarter end)
        mask = (pdf["date"] >= pe_date) & (pdf["date"] <= pe_date + pd.Timedelta(days=10))
        if mask.any():
            close_price = pdf.loc[mask, "close"].iloc[0]
        else:
            # Fall back to last price before quarter end
            before = pdf[pdf["date"] <= pe_date]
            if before.empty:
                continue
            close_price = before["close"].iloc[-1]

        row = {"period_ending": pe_date, "close_price": close_price}

        # PE from TTM EPS (convert EPS to price currency via fx_rate)
        eps = inc_row.get("diluted_earnings_per_share")
        if eps is not None and not pd.isna(eps) and eps != 0:
            row["pe"] = round(close_price / (eps / fx_rate), 2)
        else:
            row["pe"] = None

        # PB from balance sheet
        if not bal.empty:
            bal_match = bal[bal["period_ending"] == pe_date]
            if not bal_match.empty:
                equity_col = (
                    "total_equity_non_controlling_interests"
                    if "total_equity_non_controlling_interests" in bal.columns
                    else "total_equity"
                    if "total_equity" in bal.columns
                    else None
                )
                shares_col = (
                    "common_stock_shares_outstanding"
                    if "common_stock_shares_outstanding" in bal.columns
                    else "share_issued"
                    if "share_issued" in bal.columns
                    else None
                )
                if equity_col and shares_col:
                    equity = bal_match.iloc[0].get(equity_col)
                    shares = bal_match.iloc[0].get(shares_col)
                    if (
                        equity is not None
                        and shares is not None
                        and not pd.isna(equity)
                        and not pd.isna(shares)
                        and shares > 0
                    ):
                        bvps = equity / shares / fx_rate
                        if bvps > 0:
                            row["pb"] = round(close_price / bvps, 2)

                # EV/EBITDA (debt, cash, ebitda all in reporting currency)
                debt_val = bal_match.iloc[0].get("total_debt")
                cash_val = bal_match.iloc[0].get("cash_and_cash_equivalents")
                ebitda_ttm = inc_row.get("ebitda")
                if shares_col:
                    shares_val = bal_match.iloc[0].get(shares_col)
                    if (
                        shares_val
                        and not pd.isna(shares_val)
                        and shares_val > 0
                        and ebitda_ttm
                        and not pd.isna(ebitda_ttm)
                        and ebitda_ttm > 0
                    ):
                        market_cap = close_price * shares_val
                        debt_num = debt_val / fx_rate if debt_val and not pd.isna(debt_val) else 0
                        cash_num = cash_val / fx_rate if cash_val and not pd.isna(cash_val) else 0
                        ev = market_cap + debt_num - cash_num
                        row["ev_ebitda"] = round(ev / (ebitda_ttm / fx_rate), 2)

        # FCF Yield from TTM cash flow
        if not cf_ttm.empty:
            cf_match = cf_ttm[cf_ttm["period_ending"] == pe_date]
            if not cf_match.empty and "free_cash_flow" in cf_ttm.columns:
                fcf = cf_match.iloc[0].get("free_cash_flow")
                if not bal.empty:
                    bal_match2 = bal[bal["period_ending"] == pe_date]
                    shares_col2 = (
                        "common_stock_shares_outstanding"
                        if "common_stock_shares_outstanding" in bal.columns
                        else "share_issued"
                        if "share_issued" in bal.columns
                        else None
                    )
                    if not bal_match2.empty and shares_col2:
                        shares_val2 = bal_match2.iloc[0].get(shares_col2)
                        if (
                            fcf is not None
                            and not pd.isna(fcf)
                            and shares_val2
                            and not pd.isna(shares_val2)
                            and shares_val2 > 0
                        ):
                            market_cap2 = close_price * shares_val2
                            if market_cap2 > 0:
                                row["fcf_yield"] = round((fcf / fx_rate) / market_cap2 * 100, 2)

        row.setdefault("pe", None)
        row.setdefault("pb", None)
        row.setdefault("ev_ebitda", None)
        row.setdefault("fcf_yield", None)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values("period_ending").reset_index(drop=True)
    return result


def compute_growth_rates(income_df: pd.DataFrame) -> pd.DataFrame:
    """Compute QoQ and YoY revenue/EPS growth rates from quarterly income data.

    Args:
        income_df: Quarterly income statement with period_ending,
            total_revenue, diluted_earnings_per_share columns.

    Returns:
        DataFrame with columns: period_ending, revenue, eps,
        rev_qoq, rev_yoy, eps_qoq, eps_yoy, rev_accelerating.
    """
    if income_df is None or income_df.empty or "period_ending" not in income_df.columns:
        return pd.DataFrame()

    df = income_df.copy()
    df["period_ending"] = pd.to_datetime(df["period_ending"])
    df = df.sort_values("period_ending").reset_index(drop=True)

    result = df[["period_ending"]].copy()
    if "total_revenue" in df.columns:
        result["revenue"] = df["total_revenue"]
        result["rev_qoq"] = df["total_revenue"].pct_change() * 100
        result["rev_yoy"] = df["total_revenue"].pct_change(4) * 100
        # Acceleration: current YoY growth > previous quarter's YoY growth
        result["rev_accelerating"] = result["rev_yoy"].diff() > 0
    if "diluted_earnings_per_share" in df.columns:
        result["eps"] = df["diluted_earnings_per_share"]
        result["eps_qoq"] = df["diluted_earnings_per_share"].pct_change() * 100
        result["eps_yoy"] = df["diluted_earnings_per_share"].pct_change(4) * 100

    return result


def normalize_price_series(price_dfs: dict[str, pd.DataFrame], base: float = 100.0) -> pd.DataFrame:
    """Normalize multiple price series to a common base for comparison.

    Args:
        price_dfs: Dict mapping symbol → DataFrame with [date, close].
        base: Starting value (default 100).

    Returns:
        DataFrame with date index and one column per symbol, all starting at base.
    """
    if not price_dfs:
        return pd.DataFrame()

    series = {}
    for sym, df in price_dfs.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        d = d.sort_values("date").set_index("date")
        closes = d["close"].astype(float)
        if closes.iloc[0] != 0:
            series[sym] = closes / closes.iloc[0] * base
        else:
            series[sym] = closes

    if not series:
        return pd.DataFrame()

    return pd.DataFrame(series).sort_index()


def summarize_insider_activity(trades_df: pd.DataFrame) -> dict:
    """Summarize insider trading activity.

    Args:
        trades_df: DataFrame with columns like transaction_date,
            acquisition_or_disposition, securities_transacted, security_title,
            owner_name, etc.

    Returns:
        Dict with buy/sell counts, net shares, top insiders, and recent trades.
    """
    if trades_df is None or trades_df.empty:
        return {"total_trades": 0}

    df = trades_df.copy()

    # Normalize date column
    date_col = None
    for c in ["transaction_date", "filing_date", "date"]:
        if c in df.columns:
            date_col = c
            break
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(date_col, ascending=False)

    # Count buys vs sells
    acq_col = None
    for c in ["acquisition_or_disposition", "transaction_type"]:
        if c in df.columns:
            acq_col = c
            break

    buys = 0
    sells = 0
    if acq_col:
        buys = len(df[df[acq_col].str.upper().str.startswith("A", na=False)])
        sells = len(df[df[acq_col].str.upper().str.startswith("D", na=False)])

    # Net shares
    shares_col = None
    for c in ["securities_transacted", "shares", "number_of_shares"]:
        if c in df.columns:
            shares_col = c
            break

    net_shares = None
    if shares_col and acq_col:
        df["_signed_shares"] = df.apply(
            lambda r: (
                (
                    r[shares_col]
                    if str(r.get(acq_col, "")).upper().startswith("A")
                    else -r[shares_col]
                )
                if pd.notna(r.get(shares_col))
                else 0
            ),
            axis=1,
        )
        net_shares = int(df["_signed_shares"].sum())

    # Top insiders
    owner_col = None
    for c in ["owner_name", "reporting_owner", "insider_name"]:
        if c in df.columns:
            owner_col = c
            break

    top_insiders = []
    if owner_col:
        top = df[owner_col].value_counts().head(5)
        top_insiders = [{"name": name, "trades": int(count)} for name, count in top.items()]

    # Recent trades (last 5)
    recent = []
    for _, row in df.head(5).iterrows():
        entry = {}
        if owner_col and pd.notna(row.get(owner_col)):
            entry["owner"] = row[owner_col]
        if date_col and pd.notna(row.get(date_col)):
            entry["date"] = row[date_col].strftime("%Y-%m-%d")
        if acq_col and pd.notna(row.get(acq_col)):
            entry["type"] = "Buy" if str(row[acq_col]).upper().startswith("A") else "Sell"
        if shares_col and pd.notna(row.get(shares_col)):
            entry["shares"] = int(row[shares_col])
        if entry:
            recent.append(entry)

    return {
        "total_trades": len(df),
        "buys": buys,
        "sells": sells,
        "net_shares": net_shares,
        "top_insiders": top_insiders,
        "recent_trades": recent,
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
