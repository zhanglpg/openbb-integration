# OpenBB Dashboard — Improvement Ideas

**Created:** 2026-03-19  
**Status:** Backlog (prioritized)  
**Owner:** Liping / OpenClaw Finance Team

---

## 🎯 High-Impact Improvements (Quick Wins)

### 1. Data Freshness Indicators 🔴 **HIGH PRIORITY**

**Current:** No visibility into when data was last updated

**Proposed:**
- "Last updated: 2 hours ago" badge on each page/widget
- Color-coded staleness:
  - 🟢 Green = <1 hour (fresh)
  - 🟡 Yellow = 1-24 hours
  - 🔴 Red = >24 hours (stale)
- Manual refresh button per widget

**Effort:** 1-2 hours  
**Impact:** Critical for trading decisions — users need to know if data is stale

**Files to modify:**
- `dashboard.py` — Add freshness badge to portfolio table
- `pages/2_Economy.py` — Add last-updated timestamps
- `shared.py` — Add `get_data_freshness()` helper

---

### 2. Portfolio Allocation Pie Chart 🔴 **HIGH PRIORITY**

**Current:** Just prices and changes

**Proposed:**
- Pie chart showing portfolio allocation by sector
- Optional: by individual holding
- P/L tracking (requires cost basis input)
- Day's total P&L metric

**Effort:** 2-3 hours  
**Impact:** Instant visibility into portfolio composition and performance

**Files to modify:**
- `dashboard.py` — Add pie chart widget
- `src/database.py` — Add cost_basis field to holdings table
- `data/holdings.json` — New file for position tracking

---

### 3. Technical Indicators (RSI, MACD, Bollinger Bands) 🟡 **MEDIUM PRIORITY**

**Current:** Only SMA overlays + volume

**Proposed:**
- RSI (Relative Strength Index) — overbought/oversold signals
- MACD — trend momentum
- Bollinger Bands — volatility bands

**Effort:** 3-4 hours  
**Impact:** Technical traders need these for entry/exit signals

**Files to modify:**
- `pages/5_Charts.py` — Add indicator selector
- `src/analysis.py` — Add `compute_rsi()`, `compute_macd()`, `compute_bollinger_bands()`

---

## 📊 Medium-Impact Improvements

### 4. Economy Page — Event Calendar

**Proposed:**
- Fed meeting dates
- CPI/NFP release dates
- Earnings calendar for holdings

**Effort:** 2-3 hours  
**Impact:** Context for market moves

**Files to modify:**
- `pages/2_Economy.py` — Add calendar widget
- `src/economic_calendar.py` — New module for event data

---

### 5. Reports Page — Export Options

**Proposed:**
- Export to PDF
- Email to yourself
- Slack/Discord webhook integration

**Effort:** 3-4 hours  
**Impact:** Shareable briefs for team/investors

**Files to modify:**
- `pages/3_Reports.py` — Add export buttons
- `src/report_exporter.py` — New module for PDF/email/webhook

---

### 6. Research Page — Peer Comparison Charts

**Current:** Text-based peer comparison

**Proposed:**
- Side-by-side price performance chart (normalized to 100)
- Valuation comparison bar chart (PE, PB, EV/EBITDA)

**Effort:** 2-3 hours  
**Impact:** Visual comparison is faster to digest

**Files to modify:**
- `pages/4_Research.py` — Add chart tabs to comparison view

---

### 7. Global — Alerts Management Page

**Proposed:**
- New page: `/Alerts`
- Configure price alerts (e.g., "NVDA > $200")
- Configure technical alerts (e.g., "SMA crossover")
- Email/push notification settings

**Effort:** 4-6 hours  
**Impact:** Proactive monitoring instead of reactive

**Files to create:**
- `pages/6_Alerts.py` — New page
- `src/alerts.py` — Alert engine (already partially exists)
- `data/alerts.json` — Alert configuration storage

---

## 🚀 Advanced Improvements (Phase 4)

### 8. Portfolio Page — Scenario Analysis

**Proposed:**
- "What-if" calculator (e.g., "What if NVDA drops 10%?")
- Portfolio beta, Sharpe ratio, max drawdown
- Stress test scenarios (2008, 2020, etc.)

**Effort:** 6-8 hours  
**Impact:** Risk management

**Files to modify:**
- `dashboard.py` — Add scenario calculator widget
- `src/risk_analysis.py` — New module for portfolio metrics

---

### 9. Charts Page — Comparison Overlay

**Proposed:**
- Overlay multiple symbols (e.g., NVDA vs. AMD vs. SOXX)
- Normalize to 100 base for easy comparison

**Effort:** 3-4 hours  
**Impact:** Relative performance analysis

**Files to modify:**
- `pages/5_Charts.py` — Add symbol multi-select
- `src/analysis.py` — Add `normalize_prices()` helper

---

### 10. Global — Dark/Light Theme Toggle

**Proposed:**
- Theme switcher in sidebar
- Persist preference in local storage

**Effort:** 1-2 hours  
**Impact:** Eye comfort, personal preference

**Files to modify:**
- `shared.py` — Add theme configuration
- All pages — Use theme-aware colors

---

### 11. Reports Page — Auto-Schedule

**Proposed:**
- Schedule daily/weekly brief email
- Choose recipients
- Choose sections to include

**Effort:** 4-6 hours  
**Impact:** Automation

**Files to modify:**
- `pages/3_Reports.py` — Add schedule settings
- `src/scheduled_reports.py` — New module for cron/email

---

## 📋 Implementation Priority

| Priority | Improvement | Effort | Impact | Status |
|----------|-------------|--------|--------|--------|
| **1** | Data Freshness Indicators | 1-2h | 🔴 High | ⚪ Backlog |
| **2** | Portfolio Allocation Pie Chart | 2-3h | 🔴 High | ⚪ Backlog |
| **3** | Technical Indicators (RSI, MACD) | 3-4h | 🟡 Medium | ⚪ Backlog |
| **4** | Event Calendar (Economy) | 2-3h | 🟡 Medium | ⚪ Backlog |
| **5** | Export Options (Reports) | 3-4h | 🟡 Medium | ⚪ Backlog |
| **6** | Peer Comparison Charts | 2-3h | 🟡 Medium | ⚪ Backlog |
| **7** | Alerts Management Page | 4-6h | 🔴 High | ⚪ Backlog |
| **8** | Scenario Analysis | 6-8h | 🟢 Low | ⚪ Future |
| **9** | Comparison Overlay | 3-4h | 🟢 Low | ⚪ Future |
| **10** | Dark/Light Theme | 1-2h | 🟢 Low | ⚪ Future |
| **11** | Auto-Schedule Reports | 4-6h | 🟢 Low | ⚪ Future |

---

## 📝 Notes

- **Quick wins first:** Start with data freshness (1-2h) — high impact, low effort
- **User feedback:** Track which features get used most before investing in advanced features
- **Mobile responsiveness:** All pages are desktop-first; consider responsive CSS if iPad usage increases

---

## 🔗 Related Documents

- `project.md` — Main project tracking
- `CLAUDE.md` — Development guidelines
- `DASHBOARD.md` — Dashboard user guide

---

*Last updated: 2026-03-19 by Wanxia (Finance Agent)*
