"""Composite research functions — pure analysis over multiple data sources.

Each function takes pre-computed analysis data and returns structured dicts.
No DB access, no OpenBB imports.
"""


def compare_peers(
    symbols: list[str],
    technicals: dict[str, dict],
    fundamentals: list[dict],
    risk_data: dict,
) -> dict:
    """Compare a group of peer symbols across technicals, fundamentals, and risk.

    Args:
        symbols: Symbols to compare.
        technicals: Symbol → technicals dict.
        fundamentals: Valuation screen results (list of dicts).
        risk_data: Portfolio risk summary.

    Returns:
        Dict with peer comparison tables and rankings.
    """
    peer_technicals = []
    for sym in symbols:
        tech = technicals.get(sym, {})
        if "error" in tech:
            continue
        peer_technicals.append(
            {
                "symbol": sym,
                "total_return_pct": tech.get("total_return_pct"),
                "daily_volatility": tech.get("daily_volatility"),
                "max_drawdown_pct": tech.get("max_drawdown_pct"),
                "price_vs_sma20": tech.get("price_vs_sma20"),
                "volume_trend_ratio": tech.get("volume_trend_ratio"),
            }
        )

    peer_fundamentals = [f for f in fundamentals if f.get("symbol") in symbols]

    per_symbol_risk = risk_data.get("per_symbol", [])
    peer_risk = [r for r in per_symbol_risk if r.get("symbol") in symbols]

    # Rankings
    return_ranked = sorted(
        [t for t in peer_technicals if t.get("total_return_pct") is not None],
        key=lambda x: x["total_return_pct"],
        reverse=True,
    )
    vol_ranked = sorted(
        [t for t in peer_technicals if t.get("daily_volatility") is not None],
        key=lambda x: x["daily_volatility"],
    )

    pe_ranked = sorted(
        [f for f in peer_fundamentals if f.get("pe_ratio") is not None],
        key=lambda x: x["pe_ratio"],
    )

    return {
        "symbols": symbols,
        "technicals": peer_technicals,
        "fundamentals": peer_fundamentals,
        "risk": peer_risk,
        "rankings": {
            "by_return": [t["symbol"] for t in return_ranked],
            "by_volatility_asc": [t["symbol"] for t in vol_ranked],
            "by_pe_asc": [f["symbol"] for f in pe_ranked],
        },
    }


def analyze_symbol_deep(
    symbol: str,
    technicals: dict,
    fundamentals: dict,
    sec_filings: list[dict],
    peer_context: dict | None = None,
) -> dict:
    """Deep dive analysis for a single symbol.

    Args:
        symbol: The ticker symbol.
        technicals: Technicals dict for this symbol.
        fundamentals: Fundamentals dict for this symbol.
        sec_filings: Recent SEC filings for this symbol.
        peer_context: Optional peer comparison result for context.

    Returns:
        Comprehensive analysis dict.
    """
    result = {"symbol": symbol}

    # Technical summary
    if "error" not in technicals:
        result["technical_summary"] = {
            "trend": technicals.get("price_vs_sma20", "unknown"),
            "total_return_pct": technicals.get("total_return_pct"),
            "max_drawdown_pct": technicals.get("max_drawdown_pct"),
            "daily_volatility": technicals.get("daily_volatility"),
            "volume_trend": technicals.get("volume_trend_ratio"),
        }
    else:
        result["technical_summary"] = {"error": technicals.get("error")}

    # Fundamental summary
    if fundamentals:
        result["fundamental_summary"] = {
            "pe_ratio": fundamentals.get("pe_ratio"),
            "pb_ratio": fundamentals.get("pb_ratio"),
            "market_cap": fundamentals.get("market_cap"),
            "eps": fundamentals.get("eps"),
            "revenue": fundamentals.get("revenue"),
            "debt_to_equity": fundamentals.get("debt_to_equity"),
            "dividend_yield": fundamentals.get("dividend_yield"),
            "free_cash_flow": fundamentals.get("free_cash_flow"),
        }
    else:
        result["fundamental_summary"] = {}

    # SEC filing summary
    filing_types = {}
    for f in sec_filings:
        ft = f.get("report_type", "unknown")
        filing_types[ft] = filing_types.get(ft, 0) + 1
    result["sec_summary"] = {
        "total_filings": len(sec_filings),
        "by_type": filing_types,
        "most_recent": sec_filings[0] if sec_filings else None,
    }

    # Peer context
    if peer_context and peer_context.get("rankings"):
        rankings = peer_context["rankings"]
        peer_position = {}
        for rank_name, rank_list in rankings.items():
            if symbol in rank_list:
                peer_position[rank_name] = {
                    "rank": rank_list.index(symbol) + 1,
                    "total": len(rank_list),
                }
        result["peer_position"] = peer_position

    result["signals"] = _generate_signals(
        result.get("technical_summary", {}),
        result.get("fundamental_summary", {}),
    )
    return result


def _generate_signals(tech_sum: dict, fund_sum: dict) -> list[str]:
    """Generate human-readable signals from technical and fundamental summaries."""
    signals = []
    trend = tech_sum.get("trend")
    if trend == "below":
        signals.append("Trading below SMA-20 — potential weakness")
    elif trend == "above":
        signals.append("Trading above SMA-20 — positive momentum")

    vol_trend = tech_sum.get("volume_trend")
    if vol_trend is not None and vol_trend > 1.5:
        signals.append(f"Volume surge: {vol_trend:.1f}x recent average")

    drawdown = tech_sum.get("max_drawdown_pct")
    if drawdown is not None and drawdown < -10:
        signals.append(f"Significant drawdown: {drawdown:.1f}%")

    pe = fund_sum.get("pe_ratio")
    if pe is not None:
        if pe < 15:
            signals.append(f"Low PE ({pe:.1f}) — potentially undervalued")
        elif pe > 50:
            signals.append(f"High PE ({pe:.1f}) — premium valuation")
    return signals


def assess_macro_risks(
    macro_snapshot: dict,
    portfolio_risk: dict,
    sector_exposures: dict | None = None,
) -> dict:
    """Assess portfolio-level macro risks.

    Args:
        macro_snapshot: Macro environment snapshot.
        portfolio_risk: Portfolio risk summary.
        sector_exposures: Optional dict of sector → weight.

    Returns:
        Risk assessment with concerns and recommendations.
    """
    concerns = []
    risk_level = "low"

    # Yield curve
    yc = macro_snapshot.get("yield_curve_status")
    if yc == "inverted":
        concerns.append(
            {
                "factor": "yield_curve",
                "status": "inverted",
                "impact": "Historically precedes recessions — consider defensive positioning",
            }
        )
        risk_level = "elevated"

    # VIX
    vix = macro_snapshot.get("vix_regime")
    if vix == "high":
        concerns.append(
            {
                "factor": "volatility",
                "status": "VIX high",
                "impact": "Market stress elevated — wider price swings expected",
            }
        )
        risk_level = "high"
    elif vix == "medium":
        concerns.append(
            {
                "factor": "volatility",
                "status": "VIX moderate",
                "impact": "Normal market conditions with some uncertainty",
            }
        )

    # Rate direction
    rates = macro_snapshot.get("rate_direction")
    if rates == "rising":
        concerns.append(
            {
                "factor": "interest_rates",
                "status": "rising",
                "impact": "Rising rates pressure growth stock valuations",
            }
        )
        if risk_level == "low":
            risk_level = "moderate"

    # Portfolio concentration
    concentration = portfolio_risk.get("portfolio", {}).get("sector_concentration", {})
    max_sector_pct = max(concentration.values()) if concentration else 0
    if max_sector_pct > 50:
        max_sector = max(concentration, key=concentration.get)
        concerns.append(
            {
                "factor": "concentration",
                "status": f"{max_sector} at {max_sector_pct:.0f}%",
                "impact": "Heavy sector concentration increases idiosyncratic risk",
            }
        )

    # Correlation
    avg_corr = portfolio_risk.get("portfolio", {}).get("avg_pairwise_correlation")
    if avg_corr is not None and avg_corr > 0.7:
        concerns.append(
            {
                "factor": "correlation",
                "status": f"avg {avg_corr:.2f}",
                "impact": "High portfolio correlation — limited diversification benefit",
            }
        )

    return {
        "overall_risk_level": risk_level,
        "concerns": concerns,
        "macro_environment": {
            "yield_curve": yc,
            "vix_regime": vix,
            "rate_direction": rates,
        },
        "portfolio_stats": {
            "avg_correlation": avg_corr,
            "sector_concentration": concentration,
            "most_volatile": portfolio_risk.get("most_volatile_3", []),
        },
    }


def _score_opportunity(val: dict, tech: dict) -> tuple[int, list[str]]:
    """Compute opportunity score and reasons from valuation and technical data."""
    score = 0
    reasons = []

    if tech.get("price_vs_sma20") == "below":
        score += 2
        reasons.append("Below SMA-20")

    total_ret = tech.get("total_return_pct")
    if total_ret is not None and total_ret < -5:
        score += 2
        reasons.append(f"Down {total_ret:.1f}% recently")
    elif total_ret is not None and total_ret < 0:
        score += 1
        reasons.append(f"Down {total_ret:.1f}%")

    pe = val["pe_ratio"]
    if pe < 15:
        score += 2
        reasons.append(f"Low PE ({pe:.1f})")
    elif pe < 20:
        score += 1
        reasons.append(f"Moderate PE ({pe:.1f})")

    fcf_yield = val.get("fcf_yield")
    if fcf_yield is not None and fcf_yield > 0.05:
        score += 1
        reasons.append(f"Strong FCF yield ({fcf_yield:.1%})")

    drawdown = tech.get("max_drawdown_pct")
    if drawdown is not None and drawdown > -5:
        score += 1
        reasons.append("Limited drawdown")

    return score, reasons


def screen_opportunities(
    valuations: list[dict],
    technicals: dict[str, dict],
    max_pe: float = 30.0,
) -> list[dict]:
    """Screen for symbols that are technically oversold but fundamentally reasonable.

    Args:
        valuations: Valuation screen results.
        technicals: Symbol → technicals dict.
        max_pe: Maximum PE ratio to consider.

    Returns:
        List of opportunity dicts, sorted by score (best first).
    """
    opportunities = []

    for val in valuations:
        symbol = val.get("symbol")
        if not symbol:
            continue
        pe = val.get("pe_ratio")
        if pe is None or pe <= 0 or pe > max_pe:
            continue
        tech = technicals.get(symbol, {})
        if "error" in tech:
            continue

        score, reasons = _score_opportunity(val, tech)
        if score >= 2:
            opportunities.append(
                {
                    "symbol": symbol,
                    "score": score,
                    "pe_ratio": pe,
                    "total_return_pct": tech.get("total_return_pct"),
                    "price_vs_sma20": tech.get("price_vs_sma20"),
                    "fcf_yield": val.get("fcf_yield"),
                    "reasons": reasons,
                }
            )

    opportunities.sort(key=lambda x: x["score"], reverse=True)
    return opportunities
