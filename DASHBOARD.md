# 📊 Portfolio Dashboard - Quick Start

## What It Is

Interactive Streamlit dashboard for real-time portfolio monitoring with:
- **Portfolio Overview** - All holdings with prices and daily changes
- **Price Charts** - 90-day historical price visualization
- **Economic Indicators** - VIX, Treasury rates, yield curve, unemployment
- **SEC Filings** - Latest 10-K, 10-Q, 8-K filings per company

## Your Portfolio

| Sector | Holdings |
|--------|----------|
| Tech | GOOGL, NVDA |
| Semiconductors | TSMC |
| China Internet | BABA |
| US Broad Market | SPY |
| China Indices | FXI, KWEB |

---

## Quick Start

### 1. Activate Environment
```bash
cd ~/.openbb_platform
source .venv/bin/activate
```

### 2. Run Dashboard
```bash
streamlit run dashboard.py
```

### 3. Open Browser
Dashboard opens automatically at: **http://localhost:8501**

---

## Features

### Sidebar Controls
- **🔄 Refresh Data** - Fetch latest prices, fundamentals, SEC filings
- **🔃 Reset Cache** - Clear cached data
- **Select Symbol** - Choose which stock to view in detail

### Portfolio Overview Table
- Real-time prices for all 7 holdings
- Color-coded changes (green = up, red = down)
- Organized by sector

### Price History Chart
- 90-day price history
- Close and open price lines
- Key stats: latest price, 90-day change, data points

### Economic Indicators Panel
- **VIX** - Market volatility (fear gauge)
- **10Y Treasury** - Benchmark interest rate
- **Yield Curve (10Y-2Y)** - Recession indicator
- **Fed Funds Rate** - Federal Reserve policy rate

### SEC Filings Viewer
- Latest 10-K (annual), 10-Q (quarterly), 8-K (current reports)
- Clickable links to full filings on SEC.gov

---

## Data Sources

| Data Type | Provider | Refresh |
|-----------|----------|---------|
| Stock Prices | Yahoo Finance | On-demand |
| Fundamentals | FMP / Yahoo | Daily |
| SEC Filings | SEC EDGAR | Every 6 hours |
| Economic Indicators | FRED | Daily |

---

## Troubleshooting

### Dashboard won't start
```bash
# Reinstall streamlit
source .venv/bin/activate
pip install streamlit --upgrade
```

### No data showing
1. Click **🔄 Refresh Data** in sidebar
2. Wait for pipeline to complete (~30 seconds)
3. Refresh browser page

### Port 8501 already in use
```bash
# Run on different port
streamlit run dashboard.py --server.port 8502
```

---

## Keyboard Shortcuts

- `R` - Refresh data
- `C` - Clear cache
- `D` - Deploy menu (if hosting)

---

## Next Steps

1. **Bookmark** http://localhost:8501 for quick access
2. **Add to startup** - Auto-start dashboard on boot (optional)
3. **Expand watchlist** - Add more symbols in `src/config.py`

---

**Built with:** Streamlit + OpenBB SDK + Yahoo Finance + FRED + SEC EDGAR
