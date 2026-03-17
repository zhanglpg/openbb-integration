# Project: Daily Portfolio Brief Enhancement

## Goal
Merge OpenBB quantitative data with existing news-driven brief generation to create a **data-driven daily investment brief** with quantitative rigor + qualitative narrative.

---

## Context
- **Initiated:** 2026-03-16 by Liping
- **Strategic fit:** Leverages Phase 2 (data pipeline) + Phase 3 (MCP/analysis) for production reporting
- **Current gap:** Brief has news/narrative but no quantitative data; OpenBB has data but no narrative synthesis

---

## Current State Analysis

| System | Strengths | Gaps |
|--------|-----------|------|
| **Current Brief** (`~/.openclaw/skills/custom/briefs/`) | ✅ News aggregation (RSS, Twitter, web)<br>✅ LLM synthesis<br>✅ Portfolio context | ❌ No quantitative data<br>❌ No technical analysis<br>❌ No valuation metrics<br>❌ No risk calculations<br>❌ Manual news interpretation |
| **OpenBB Platform** (`~/.openbb_platform/`) | ✅ Real-time prices & fundamentals<br>✅ Technical indicators (SMA, volatility, drawdown)<br>✅ Risk metrics (Sharpe, correlation)<br>✅ SEC filings tracking<br>✅ Economic indicators (FRED)<br>✅ Peer comparison | ❌ No news aggregation<br>❌ No LLM narrative synthesis<br>❌ Reports are data-only (no storytelling) |

---

## Enhancement Vision

**Merge the best of both:**
- **OpenBB** → Quantitative data, technicals, risk, fundamentals
- **Current Brief** → News, narrative, LLM synthesis, actionable insights

**Result:** A **data-driven daily brief** with quantitative rigor + qualitative context.

---

## Implementation Plan (4 Phases)

### Phase 1: Data Integration (Week 1) 🔄 **IN PROGRESS** (started 2026-03-17)

**Goal:** Feed OpenBB data into the brief generation pipeline

| Task | Description | Effort | Status |
|------|-------------|--------|--------|
| **1.1** | Create OpenBB data exporter | Export portfolio overview, technicals, risk to JSON | 1-2 hours | ⚪ |
| **1.2** | Update brief config | Add OpenBB data paths to `config.portfolio.json` | 30 min | ⚪ |
| **1.3** | Modify brief generator | Load OpenBB JSON before LLM synthesis | 2-3 hours | ⚪ |
| **1.4** | Test end-to-end | Generate brief with OpenBB data | 1 hour | ⚪ |

**Deliverable:** Brief includes OpenBB price data, technicals, risk metrics

---

### Phase 2: Enhanced Sections (Week 2)

**Goal:** Add new data-rich sections to the brief

| New Section | OpenBB Data Source | Example Content | Status |
|-------------|-------------------|-----------------|--------|
| **Portfolio Snapshot** | `get_portfolio_overview()` | Table with prices, changes, volume | ⚪ |
| **Technical Signals** | `compute_price_technicals()` | SMA positions, volatility, drawdown | ⚪ |
| **Valuation Check** | `compute_valuation_screen()` | PE, PB, PEG vs. historical averages | ⚪ |
| **Risk Dashboard** | `compute_portfolio_risk()` | Most volatile, correlation, Sharpe | ⚪ |
| **Macro Snapshot** | `compute_macro_snapshot()` | Yield curve, VIX regime, Fed direction | ⚪ |
| **SEC Activity** | `get_sec_filings()` | Recent 8-K/10-Q filings for holdings | ⚪ |

**Effort:** 4-6 hours

**Deliverable:** Brief has 6 new quantitative sections

---

### Phase 3: Smart Alerts (Week 3)

**Goal:** Automated alerts based on thresholds

| Alert Type | Trigger | Action | Status |
|------------|---------|--------|--------|
| **Price Alert** | >5% daily move (stock), >3% (ETF) | Flag in brief + Discord notification | ⚪ |
| **Technical Alert** | Crosses SMA-20, volume spike >2x | Add to "Technical Signals" section | ⚪ |
| **Valuation Alert** | PE < historical avg by 20% | Flag as "Potential Opportunity" | ⚪ |
| **Risk Alert** | VIX >25, correlation >0.7 | Add to "Risk Dashboard" | ⚪ |
| **SEC Alert** | 8-K filing for portfolio stock | Add to "SEC Activity" with summary | ⚪ |

**Implementation:**
```python
# In brief generator
from openbb_platform.src.analysis import compute_alerts

alerts = compute_alerts(
    portfolio_overview=data,
    technicals=tech_data,
    thresholds=ALERT_THRESHOLDS
)
```

**Effort:** 3-4 hours

**Deliverable:** Automated, threshold-based alerts in brief

---

### Phase 4: MCP-Powered Insights (Week 4)

**Goal:** Use MCP server for dynamic analysis

| MCP Tool | Brief Integration | Status |
|----------|-------------------|--------|
| `analyze_symbol()` | Deep dive on top/bottom performers | ⚪ |
| `compare_peers()` | Peer context for portfolio stocks | ⚪ |
| `get_daily_report()` | Pre-formatted report sections | ⚪ |
| `screen_opportunities()` | "Watchlist Opportunities" section | ⚪ |

**Example Brief Section:**
```markdown
## Deep Dive: NVDA (Top Performer)
- **Trend:** Above SMA-20, bullish momentum
- **Valuation:** PE 65x vs. peer avg 45x — premium justified by AI growth
- **Peer Context:** Outperforming AMD (+2%) and INTC (-5%)
- **Catalyst:** GTC event this week — monitor for AI announcements
```

**Effort:** 4-6 hours

**Deliverable:** AI-generated insights powered by OpenBB data

---

## File Changes Required

| File | Change | Status |
|------|--------|--------|
| `~/.openclaw/skills/custom/briefs/scripts/generate_brief.py` | Add OpenBB data loading, new sections | ⚪ |
| `~/.openclaw/skills/custom/briefs/config.portfolio.json` | Add OpenBB paths, alert thresholds | ⚪ |
| `~/.openclaw/skills/custom/briefs/templates/portfolio-brief.md` | Add new section templates | ⚪ |
| `~/.openbb_platform/src/brief_exporter.py` | **NEW** — Export OpenBB data to JSON | ⚪ |
| `~/.openbb_platform/src/alerts.py` | **NEW** — Alert generation logic | ⚪ |

---

## Implementation Timeline

```
Week 1: Data Integration
├── Day 1: Create brief_exporter.py
├── Day 2: Update brief generator to load OpenBB data
└── Day 3: Test end-to-end

Week 2: Enhanced Sections
├── Day 1: Portfolio Snapshot + Technical Signals
├── Day 2: Valuation + Risk Dashboard
└── Day 3: Macro + SEC Activity

Week 3: Smart Alerts
├── Day 1: Implement alert logic
├── Day 2: Add Discord notifications
└── Day 3: Test alert thresholds

Week 4: MCP Integration
├── Day 1: Connect MCP tools to brief generator
├── Day 2: Add AI-generated insights
└── Day 3: Polish and document
```

---

## Sample Enhanced Brief Structure

```markdown
# Daily Portfolio Brief — YYYY-MM-DD

## 📊 Portfolio Snapshot (OpenBB)
| Symbol | Price | Change | Volume | vs. SMA-20 |
|--------|-------|--------|--------|------------|
| GOOGL  | $303  | +1.2%  | 24M    | Above ✅   |
| NVDA   | $183  | -1.5%  | 155M   | Below ⚠️   |

## 📈 Technical Signals (OpenBB)
- **Bullish:** AVGO (above SMA-20, volume +20%)
- **Bearish:** BABA (below SMA-20, max drawdown -17%)

## 💰 Valuation Check (OpenBB)
| Symbol | PE | vs. 5Y Avg | Verdict |
|--------|----|------------|---------|
| GOOGL  | 25x | -10% | Undervalued ✅ |
| NVDA   | 65x | +30% | Premium ⚠️ |

## ⚠️ Risk Dashboard (OpenBB)
- **Most Volatile:** INTC, AMD, TSM
- **Portfolio Correlation:** 0.41 (moderate)
- **VIX Regime:** High (27.29)

## 🌍 Macro Snapshot (OpenBB)
- **Yield Curve:** Normal (10Y-2Y: 0.55%)
- **Fed Funds:** Stable (3.64%)
- **Oil:** $100+ (geopolitical premium)

## 📰 News & Narrative (Current Brief)
[Existing news aggregation + LLM synthesis]

## 🚨 Smart Alerts (OpenBB + Thresholds)
- [ALERT] TSM: -5.03% — exceeds 5% threshold
- [ALERT] BABA: Below SMA-20 — bearish signal
- [INFO] GOOGL: 8-K filed — review filing

## 🔬 Deep Dive (MCP-Powered)
[AI-generated insights on top/bottom performers]
```

---

## Success Metrics

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| **Data Sources** | News only | News + Prices + Technicals + Fundamentals | ✅ |
| **Quantitative Sections** | 0 | 6 | ✅ |
| **Alert Automation** | Manual | Threshold-based | ✅ |
| **Time to Generate** | 5 min | 2 min (automated data) | ✅ |
| **Actionable Insights** | LLM-only | Data-driven + LLM | ✅ |

---

## Progress Log

| Date | Milestone | Status |
|------|-----------|--------|
| 2026-03-16 | Project plan created | ✅ |
| TBD | Phase 1 kickoff | ⚪ |
| TBD | Phase 1 complete | ⚪ |
| TBD | Phase 2 kickoff | ⚪ |
| TBD | Phase 2 complete | ⚪ |
| TBD | Phase 3 kickoff | ⚪ |
| TBD | Phase 3 complete | ⚪ |
| TBD | Phase 4 kickoff | ⚪ |
| TBD | Phase 4 complete | ⚪ |

---

## Stucks / Blockers

_None — Ready to start Phase 1_

---

## Next Actions

- [ ] **Phase 1 kickoff** — Create `brief_exporter.py`
- [ ] Update `generate_brief.py` to load OpenBB data
- [ ] Test end-to-end brief generation
- [ ] Gather feedback and iterate

---

## Resources

- **OpenBB Platform:** `~/.openbb_platform/`
- **Brief Generator:** `~/.openclaw/skills/custom/briefs/`
- **MCP Server:** `~/.openbb_platform/src/mcp_server.py`
- **Analysis Module:** `~/.openbb_platform/src/analysis.py`

---

*Last updated: 2026-03-16 by Wanxia (Finance Agent)*
