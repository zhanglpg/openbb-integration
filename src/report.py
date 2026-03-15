"""Daily report generator — pure functions, no DB access.

Composes analysis outputs into a structured morning briefing.
"""

from datetime import datetime


def generate_daily_report(
    portfolio_overview: list[dict],
    technicals: dict[str, dict],
    valuations: list[dict],
    risk_summary: dict,
    macro_snapshot: dict,
    sec_activity: dict,
    report_date: str | None = None,
) -> dict:
    """Aggregate all analysis into a structured daily report.

    Args:
        portfolio_overview: Per-symbol price and change data.
        technicals: Symbol → technicals dict from compute_price_technicals.
        valuations: Valuation screen results.
        risk_summary: Portfolio risk summary.
        macro_snapshot: Macro environment snapshot.
        sec_activity: SEC filing activity summary.
        report_date: Override date string (YYYY-MM-DD). Defaults to today.

    Returns:
        Structured report dict with all sections.
    """
    report_date = report_date or datetime.now().strftime("%Y-%m-%d")

    movers = identify_notable_movers(portfolio_overview)
    alerts = identify_alerts(technicals, macro_snapshot, sec_activity)

    # Market benchmarks (SPY/QQQ)
    benchmarks = []
    for item in portfolio_overview:
        if item.get("symbol") in ("SPY", "QQQ"):
            benchmarks.append(item)

    # Top/bottom performers
    sorted_by_change = sorted(
        [p for p in portfolio_overview if p.get("change_pct") is not None],
        key=lambda x: x["change_pct"],
        reverse=True,
    )
    top_3 = sorted_by_change[:3] if len(sorted_by_change) >= 3 else sorted_by_change
    bottom_3 = sorted_by_change[-3:] if len(sorted_by_change) >= 3 else []

    # Technical signals summary
    tech_signals = []
    for symbol, tech in technicals.items():
        if "error" in tech:
            continue
        signal = {"symbol": symbol}
        if tech.get("price_vs_sma20"):
            signal["sma20_position"] = tech["price_vs_sma20"]
        if tech.get("volume_trend_ratio") is not None:
            signal["volume_trend"] = tech["volume_trend_ratio"]
        if tech.get("total_return_pct") is not None:
            signal["total_return_pct"] = tech["total_return_pct"]
        tech_signals.append(signal)

    # Macro summary
    macro_summary = {
        "yield_curve": macro_snapshot.get("yield_curve_status"),
        "vix_regime": macro_snapshot.get("vix_regime"),
        "rate_direction": macro_snapshot.get("rate_direction"),
    }

    return {
        "date": report_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "benchmarks": benchmarks,
        "top_performers": top_3,
        "bottom_performers": bottom_3,
        "notable_movers": movers,
        "technical_signals": tech_signals,
        "macro_summary": macro_summary,
        "macro_indicators": macro_snapshot.get("indicators", []),
        "sec_recent_8k": sec_activity.get("recent_8k_activity", []),
        "risk_highlights": {
            "most_volatile": risk_summary.get("most_volatile_3", []),
            "least_volatile": risk_summary.get("least_volatile_3", []),
            "avg_correlation": risk_summary.get("portfolio", {}).get("avg_pairwise_correlation"),
        },
        "alerts": alerts,
    }


def format_report_markdown(report: dict) -> str:
    """Format a report dict as a readable Markdown document."""
    lines = []
    lines.append(f"# Daily Market Report — {report['date']}")
    lines.append(f"*Generated: {report['generated_at']}*")
    lines.append("")

    # Market Summary
    lines.append("## Market Summary")
    if report.get("benchmarks"):
        for b in report["benchmarks"]:
            chg = b.get("change_pct")
            chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
            lines.append(f"- **{b['symbol']}**: ${b.get('price', 'N/A')} ({chg_str})")
    else:
        lines.append("- No benchmark data available")
    lines.append("")

    # Top Performers
    lines.append("## Top Performers")
    for p in report.get("top_performers", []):
        chg = p.get("change_pct")
        chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
        lines.append(f"- **{p['symbol']}**: {chg_str}")
    lines.append("")

    # Bottom Performers
    lines.append("## Bottom Performers")
    for p in report.get("bottom_performers", []):
        chg = p.get("change_pct")
        chg_str = f"{chg:+.2f}%" if chg is not None else "N/A"
        lines.append(f"- **{p['symbol']}**: {chg_str}")
    lines.append("")

    # Notable Movers
    if report.get("notable_movers"):
        lines.append("## Notable Movers (>3% change)")
        for m in report["notable_movers"]:
            chg_str = f"{m['change_pct']:+.2f}%"
            lines.append(f"- **{m['symbol']}**: {chg_str}")
        lines.append("")

    # Technical Signals
    if report.get("technical_signals"):
        lines.append("## Technical Signals")
        above_sma20 = [s for s in report["technical_signals"] if s.get("sma20_position") == "above"]
        below_sma20 = [s for s in report["technical_signals"] if s.get("sma20_position") == "below"]
        volume_surges = [
            s for s in report["technical_signals"] if (s.get("volume_trend") or 0) > 1.5
        ]
        if above_sma20:
            syms = ", ".join(s["symbol"] for s in above_sma20)
            lines.append(f"- **Above SMA-20**: {syms}")
        if below_sma20:
            syms = ", ".join(s["symbol"] for s in below_sma20)
            lines.append(f"- **Below SMA-20**: {syms}")
        if volume_surges:
            for s in volume_surges:
                lines.append(
                    f"- **Volume surge**: {s['symbol']} ({s['volume_trend']:.1f}x 20d avg)"
                )
        lines.append("")

    # Macro Environment
    lines.append("## Macro Environment")
    macro = report.get("macro_summary", {})
    if macro.get("yield_curve"):
        lines.append(f"- **Yield Curve**: {macro['yield_curve']}")
    if macro.get("vix_regime"):
        lines.append(f"- **VIX Regime**: {macro['vix_regime']}")
    if macro.get("rate_direction"):
        lines.append(f"- **Fed Funds Direction**: {macro['rate_direction']}")
    lines.append("")

    # SEC Activity
    if report.get("sec_recent_8k"):
        lines.append("## Recent SEC 8-K Filings")
        for filing in report["sec_recent_8k"][:10]:
            desc = filing.get("description", "")
            desc_str = f" — {desc}" if desc else ""
            lines.append(f"- **{filing['symbol']}** ({filing['date']}){desc_str}")
        lines.append("")

    # Risk Highlights
    risk = report.get("risk_highlights", {})
    if risk.get("most_volatile") or risk.get("least_volatile"):
        lines.append("## Risk Highlights")
        if risk.get("most_volatile"):
            lines.append(f"- **Most volatile**: {', '.join(risk['most_volatile'])}")
        if risk.get("least_volatile"):
            lines.append(f"- **Least volatile**: {', '.join(risk['least_volatile'])}")
        if risk.get("avg_correlation") is not None:
            lines.append(f"- **Avg pairwise correlation**: {risk['avg_correlation']:.3f}")
        lines.append("")

    # Alerts
    if report.get("alerts"):
        lines.append("## Alerts")
        for alert in report["alerts"]:
            lines.append(f"- [{alert['severity'].upper()}] {alert['message']}")
        lines.append("")

    lines.append("---")
    lines.append("*OpenClaw Financial Intelligence*")
    return "\n".join(lines)


def identify_notable_movers(
    portfolio_overview: list[dict], threshold_pct: float = 3.0
) -> list[dict]:
    """Find symbols with absolute daily change exceeding threshold.

    Args:
        portfolio_overview: Per-symbol price data with change_pct.
        threshold_pct: Minimum absolute change % to flag.

    Returns:
        List of dicts with symbol and change_pct, sorted by absolute change desc.
    """
    movers = []
    for item in portfolio_overview:
        chg = item.get("change_pct")
        if chg is not None and abs(chg) >= threshold_pct:
            movers.append(
                {
                    "symbol": item["symbol"],
                    "change_pct": chg,
                    "price": item.get("price"),
                    "sector": item.get("sector"),
                }
            )
    movers.sort(key=lambda x: abs(x["change_pct"]), reverse=True)
    return movers


def identify_alerts(
    technicals: dict[str, dict],
    macro_snapshot: dict,
    sec_activity: dict,
) -> list[dict]:
    """Generate actionable alerts from analysis data.

    Args:
        technicals: Symbol → technicals dict.
        macro_snapshot: Macro environment snapshot.
        sec_activity: SEC filing activity summary.

    Returns:
        List of alert dicts with severity, category, and message.
    """
    alerts = []

    # SMA crossover signals
    for symbol, tech in technicals.items():
        if "error" in tech:
            continue
        sma5 = tech.get("sma_5")
        sma20 = tech.get("sma_20")
        if sma5 is not None and sma20 is not None:
            if sma5 > sma20 * 1.02:
                alerts.append(
                    {
                        "severity": "info",
                        "category": "technical",
                        "message": (
                            f"{symbol}: SMA-5 ({sma5:.2f}) well above "
                            f"SMA-20 ({sma20:.2f}) — bullish momentum"
                        ),
                    }
                )
            elif sma5 < sma20 * 0.98:
                alerts.append(
                    {
                        "severity": "warning",
                        "category": "technical",
                        "message": (
                            f"{symbol}: SMA-5 ({sma5:.2f}) well below "
                            f"SMA-20 ({sma20:.2f}) — bearish signal"
                        ),
                    }
                )

        # Volume surge
        vol_ratio = tech.get("volume_trend_ratio")
        if vol_ratio is not None and vol_ratio > 2.0:
            alerts.append(
                {
                    "severity": "info",
                    "category": "volume",
                    "message": f"{symbol}: Volume surge detected ({vol_ratio:.1f}x 20d average)",
                }
            )

        # Significant drawdown
        drawdown = tech.get("max_drawdown_pct")
        if drawdown is not None and drawdown < -15:
            alerts.append(
                {
                    "severity": "warning",
                    "category": "risk",
                    "message": f"{symbol}: Max drawdown of {drawdown:.1f}% in lookback period",
                }
            )

    # Macro alerts
    if macro_snapshot.get("yield_curve_status") == "inverted":
        alerts.append(
            {
                "severity": "warning",
                "category": "macro",
                "message": "Yield curve inverted — historically associated with recessions",
            }
        )

    if macro_snapshot.get("vix_regime") == "high":
        alerts.append(
            {
                "severity": "warning",
                "category": "macro",
                "message": "VIX in high regime — elevated market fear",
            }
        )

    # SEC alerts — recent 8-K filings
    recent_8k = sec_activity.get("recent_8k_activity", [])
    if len(recent_8k) > 5:
        alerts.append(
            {
                "severity": "info",
                "category": "sec",
                "message": f"{len(recent_8k)} recent 8-K filings — review for material events",
            }
        )

    return alerts
