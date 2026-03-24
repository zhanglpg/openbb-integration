"""Daily report generator — pure functions, no DB access.

Composes analysis outputs into a structured morning briefing.
"""

from datetime import datetime

from config import ALERT_THRESHOLDS, ETF_SYMBOLS


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
    alerts = identify_alerts(
        technicals,
        macro_snapshot,
        sec_activity,
        portfolio_snapshot=portfolio_overview,
        risk_summary=risk_summary,
        valuations=valuations,
    )

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


def _fmt_change(chg) -> str:
    """Format a percent change value."""
    return f"{chg:+.2f}%" if chg is not None else "N/A"


def _fmt_benchmarks(report: dict) -> list[str]:
    lines = ["## Market Summary"]
    if report.get("benchmarks"):
        for b in report["benchmarks"]:
            chg = _fmt_change(b.get("change_pct"))
            lines.append(f"- **{b['symbol']}**: ${b.get('price', 'N/A')} ({chg})")
    else:
        lines.append("- No benchmark data available")
    return lines


def _fmt_performers(report: dict) -> list[str]:
    lines = ["## Top Performers"]
    for p in report.get("top_performers", []):
        lines.append(f"- **{p['symbol']}**: {_fmt_change(p.get('change_pct'))}")
    lines.append("")
    lines.append("## Bottom Performers")
    for p in report.get("bottom_performers", []):
        lines.append(f"- **{p['symbol']}**: {_fmt_change(p.get('change_pct'))}")
    return lines


def _fmt_movers(report: dict) -> list[str]:
    if not report.get("notable_movers"):
        return []
    lines = ["## Notable Movers (>3% change)"]
    for m in report["notable_movers"]:
        lines.append(f"- **{m['symbol']}**: {m['change_pct']:+.2f}%")
    return lines


def _fmt_technicals(report: dict) -> list[str]:
    if not report.get("technical_signals"):
        return []
    lines = ["## Technical Signals"]
    above_sma20 = [s for s in report["technical_signals"] if s.get("sma20_position") == "above"]
    below_sma20 = [s for s in report["technical_signals"] if s.get("sma20_position") == "below"]
    volume_surges = [s for s in report["technical_signals"] if (s.get("volume_trend") or 0) > 1.5]
    if above_sma20:
        lines.append(f"- **Above SMA-20**: {', '.join(s['symbol'] for s in above_sma20)}")
    if below_sma20:
        lines.append(f"- **Below SMA-20**: {', '.join(s['symbol'] for s in below_sma20)}")
    for s in volume_surges:
        lines.append(f"- **Volume surge**: {s['symbol']} ({s['volume_trend']:.1f}x 20d avg)")
    return lines


def _fmt_macro(report: dict) -> list[str]:
    lines = ["## Macro Environment"]
    macro = report.get("macro_summary", {})
    if macro.get("yield_curve"):
        lines.append(f"- **Yield Curve**: {macro['yield_curve']}")
    if macro.get("vix_regime"):
        lines.append(f"- **VIX Regime**: {macro['vix_regime']}")
    if macro.get("rate_direction"):
        lines.append(f"- **Fed Funds Direction**: {macro['rate_direction']}")
    return lines


def _fmt_sec(report: dict) -> list[str]:
    if not report.get("sec_recent_8k"):
        return []
    lines = ["## Recent SEC 8-K Filings"]
    for filing in report["sec_recent_8k"][:10]:
        desc = filing.get("description", "")
        desc_str = f" — {desc}" if desc else ""
        lines.append(f"- **{filing['symbol']}** ({filing['date']}){desc_str}")
    return lines


def _fmt_risk(report: dict) -> list[str]:
    risk = report.get("risk_highlights", {})
    if not risk.get("most_volatile") and not risk.get("least_volatile"):
        return []
    lines = ["## Risk Highlights"]
    if risk.get("most_volatile"):
        lines.append(f"- **Most volatile**: {', '.join(risk['most_volatile'])}")
    if risk.get("least_volatile"):
        lines.append(f"- **Least volatile**: {', '.join(risk['least_volatile'])}")
    if risk.get("avg_correlation") is not None:
        lines.append(f"- **Avg pairwise correlation**: {risk['avg_correlation']:.3f}")
    return lines


def _fmt_alerts(report: dict) -> list[str]:
    if not report.get("alerts"):
        return []
    lines = ["## Alerts"]
    for alert in report["alerts"]:
        lines.append(f"- [{alert['severity'].upper()}] {alert['message']}")
    return lines


def format_report_markdown(report: dict) -> str:
    """Format a report dict as a readable Markdown document."""
    sections = [
        [f"# Daily Market Report — {report['date']}", f"*Generated: {report['generated_at']}*"],
        _fmt_benchmarks(report),
        _fmt_performers(report),
        _fmt_movers(report),
        _fmt_technicals(report),
        _fmt_macro(report),
        _fmt_sec(report),
        _fmt_risk(report),
        _fmt_alerts(report),
        ["---", "*OpenClaw Financial Intelligence*"],
    ]
    parts = []
    for section in sections:
        if section:
            parts.append("\n".join(section))
    return "\n\n".join(parts)


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


def _sma_crossover_alert(symbol: str, tech: dict, crossover_pct: float) -> dict | None:
    """Generate SMA crossover alert if SMA-5 diverges significantly from SMA-20."""
    sma5 = tech.get("sma_5")
    sma20 = tech.get("sma_20")
    if sma5 is None or sma20 is None:
        return None
    if sma5 > sma20 * (1 + crossover_pct):
        return {
            "severity": "info",
            "category": "technical",
            "message": (
                f"{symbol}: SMA-5 ({sma5:.2f}) well above SMA-20 ({sma20:.2f}) — bullish momentum"
            ),
        }
    if sma5 < sma20 * (1 - crossover_pct):
        return {
            "severity": "warning",
            "category": "technical",
            "message": (
                f"{symbol}: SMA-5 ({sma5:.2f}) well below SMA-20 ({sma20:.2f}) — bearish signal"
            ),
        }
    return None


def _technical_alerts(technicals: dict[str, dict], t: dict) -> list[dict]:
    """Generate alerts from technical indicators (SMA crossovers, volume, drawdown)."""
    alerts = []
    crossover_pct = t.get("sma_crossover_pct", 0.02)
    for symbol, tech in technicals.items():
        if "error" in tech:
            continue
        sma_alert = _sma_crossover_alert(symbol, tech, crossover_pct)
        if sma_alert:
            alerts.append(sma_alert)

        vol_ratio = tech.get("volume_trend_ratio")
        if vol_ratio is not None and vol_ratio > t.get("volume_surge_ratio", 2.0):
            alerts.append(
                {
                    "severity": "info",
                    "category": "volume",
                    "message": f"{symbol}: Volume surge detected ({vol_ratio:.1f}x 20d average)",
                }
            )

        drawdown = tech.get("max_drawdown_pct")
        if drawdown is not None and drawdown < t.get("drawdown_pct", -15):
            alerts.append(
                {
                    "severity": "warning",
                    "category": "risk",
                    "message": f"{symbol}: Max drawdown of {drawdown:.1f}% in lookback period",
                }
            )
    return alerts


def _price_movement_alerts(portfolio_snapshot: list[dict] | None, t: dict) -> list[dict]:
    """Generate alerts for significant price movements."""
    if not portfolio_snapshot:
        return []
    alerts = []
    stock_threshold = t.get("price_move_stock_pct", 5.0)
    etf_threshold = t.get("price_move_etf_pct", 3.0)
    for item in portfolio_snapshot:
        chg = item.get("change_pct")
        if chg is None:
            continue
        sym = item["symbol"]
        threshold = etf_threshold if sym in ETF_SYMBOLS else stock_threshold
        if abs(chg) > threshold:
            direction = "up" if chg > 0 else "down"
            alerts.append(
                {
                    "severity": "warning",
                    "category": "price",
                    "message": (
                        f"{sym}: {direction} {abs(chg):.1f}% — exceeds {threshold}% threshold"
                    ),
                }
            )
    return alerts


def _valuation_alerts(valuations: list[dict] | None, t: dict) -> list[dict]:
    """Flag symbols with PE well below peer median."""
    if not valuations:
        return []
    pe_values = [
        v["pe_ratio"] for v in valuations if v.get("pe_ratio") is not None and v["pe_ratio"] > 0
    ]
    if not pe_values:
        return []
    pe_values_sorted = sorted(pe_values)
    mid = len(pe_values_sorted) // 2
    if len(pe_values_sorted) % 2 == 0 and len(pe_values_sorted) >= 2:
        median_pe = (pe_values_sorted[mid - 1] + pe_values_sorted[mid]) / 2
    else:
        median_pe = pe_values_sorted[mid]
    discount_pct = t.get("valuation_pe_discount_pct", 20)
    cutoff = median_pe * (1 - discount_pct / 100)
    alerts = []
    for v in valuations:
        pe = v.get("pe_ratio")
        if pe is not None and 0 < pe < cutoff:
            alerts.append(
                {
                    "severity": "info",
                    "category": "valuation",
                    "message": (
                        f"{v['symbol']}: PE {pe:.1f} is "
                        f"{((median_pe - pe) / median_pe * 100):.0f}% below "
                        f"peer median ({median_pe:.1f})"
                    ),
                }
            )
    return alerts


def _macro_and_risk_alerts(
    macro_snapshot: dict,
    sec_activity: dict,
    risk_summary: dict | None,
    t: dict,
) -> list[dict]:
    """Generate macro, risk correlation, and SEC alerts."""
    alerts = []
    if risk_summary:
        avg_corr = risk_summary.get("portfolio", {}).get("avg_pairwise_correlation")
        if avg_corr is not None and avg_corr > t.get("correlation_high", 0.7):
            alerts.append(
                {
                    "severity": "warning",
                    "category": "risk",
                    "message": (
                        f"Portfolio avg pairwise correlation is {avg_corr:.2f} — "
                        f"high concentration risk"
                    ),
                }
            )
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
    recent_8k = sec_activity.get("recent_8k_activity", [])
    if len(recent_8k) > t.get("sec_8k_count", 5):
        alerts.append(
            {
                "severity": "info",
                "category": "sec",
                "message": f"{len(recent_8k)} recent 8-K filings — review for material events",
            }
        )
    return alerts


def identify_alerts(
    technicals: dict[str, dict],
    macro_snapshot: dict,
    sec_activity: dict,
    portfolio_snapshot: list[dict] | None = None,
    risk_summary: dict | None = None,
    valuations: list[dict] | None = None,
    thresholds: dict | None = None,
) -> list[dict]:
    """Generate actionable alerts from analysis data.

    Args:
        technicals: Symbol → technicals dict.
        macro_snapshot: Macro environment snapshot.
        sec_activity: SEC filing activity summary.
        portfolio_snapshot: Per-symbol price/change data.
        risk_summary: Portfolio risk summary.
        valuations: Valuation screen results.
        thresholds: Override alert thresholds (merged with defaults).

    Returns:
        List of alert dicts with severity, category, and message.
    """
    t = {**ALERT_THRESHOLDS, **(thresholds or {})}
    alerts = _technical_alerts(technicals, t)
    alerts.extend(_price_movement_alerts(portfolio_snapshot, t))
    alerts.extend(_valuation_alerts(valuations, t))
    alerts.extend(_macro_and_risk_alerts(macro_snapshot, sec_activity, risk_summary, t))
    return alerts
