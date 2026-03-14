---
tags: [finance, data-infrastructure, openbb, investment-research, AI-agents]
created: 2026-03-14
status: proposal
---

# OpenBB Project Proposal — Financial Data Infrastructure for AI Research

> **Connect once, consume everywhere.** OpenBB provides the data infrastructure layer for investment research, portfolio analysis, and quantitative workflows.

---

## Executive Summary

**OpenBB (Open Data Platform)** is an open-source financial data infrastructure platform that enables unified access to financial and economic data from 30+ providers through a single API. It follows a "connect once, consume everywhere" architecture, serving:

- **Quants** — Python SDK
- **Analysts** — Workspace/Excel
- **AI Agents** — MCP servers, REST APIs

**Key Value:** OpenBB provides the data infrastructure layer for investment research, portfolio analysis, and quantitative workflows—aligning with the focus on AI self-improvement via code and data-driven decision making. The platform's open-source nature (AGPLv3) and extensibility make it ideal for building custom research tools on top of.

---

## 1. What is OpenBB?

### Core Identity

| Attribute | Value |
|-----------|-------|
| **Name** | Open Data Platform by OpenBB (ODP) |
| **Mission** | Build open-source infrastructure for investment research accessible to everyone |
| **GitHub** | https://github.com/OpenBB-finance/OpenBB |
| **License** | GNU Affero General Public License v3.0 (AGPLv3) |
| **Primary Language** | Python (3.10-3.13) |

### Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Data Providers                        │
│  (FRED, Yahoo Finance, Polygon, Intrinio, SEC, etc.)    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              Open Data Platform (ODP)                    │
│  - Unified API abstraction                               │
│  - Normalized data schemas                               │
│  - Provider-agnostic queries                             │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│   Python SDK  │ │  REST API     │ │   CLI         │
│   (Quants)    │ │  (FastAPI)    │ │   (Terminal)  │
└───────────────┘ └─────────────────┘ └───────────────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          ▼
              ┌───────────────────────┐
              │  OpenBB Workspace     │
              │  (Enterprise UI)      │
              │  Excel Add-in         │
              │  MCP Servers (AI)     │
              └───────────────────────┘
```

### Core Components

1. **openbb (Python Package)** — Core SDK with router modules
2. **openbb-cli** — Command-line terminal interface
3. **openbb-api** — FastAPI-based REST server (port 6900)
4. **OpenBB Workspace** — Commercial enterprise UI (hosted or self-deployed)

---

## 2. Key Features

### 2.1 Python SDK

**Installation:**
```bash
pip install openbb              # Core + selected providers
pip install "openbb[all]"       # All extensions
```

**Usage Example:**
```python
from openbb import obb

# Login to retrieve stored API keys
obb.account.login(pat="your-personal-access-token")

# Set output preference
obb.user.preferences.output_type = "dataframe"

# Fetch historical equity data
output = obb.equity.price.historical("AAPL")
df = output.to_dataframe()

# Options chain
options = obb.derivatives.options.chains("SPY", provider="cboe")
```

### 2.2 Data Providers Supported

**Free Providers (no paid subscription required):**

| Provider | Description |
|----------|-------------|
| `yfinance` | Yahoo Finance (equities, fundamentals) |
| `fred` | Federal Reserve Economic Data |
| `sec` | SEC EDGAR filings |
| `imf` | International Monetary Fund |
| `oecd` | OECD statistics |
| `ecb` | European Central Bank |
| `cboe` | Cboe options data |
| `finra` | FINRA market data |
| `finviz` | Finviz screeners |
| `nasdaq` | Nasdaq Data Link |

**Paid/Freemium Providers:**

| Provider | Description | Minimum Tier |
|----------|-------------|--------------|
| `polygon` | Real-time market data | Free tier available |
| `fmp` | Financial Modeling Prep | Free tier |
| `benzinga` | News, calendar, fundamentals | Paid |
| `intrinio` | Institutional-grade data | Paid |
| `tradingeconomics` | Global economic indicators | Paid |
| `alpha-vantage` | API for stocks, forex, crypto | Free tier |
| `tiingo` | Institutional data platform | Free tier |

**Total:** 30+ data provider extensions available

---

## 3. Use Cases

### 3.1 Portfolio Analysis
- Historical price data across asset classes
- Fundamental analysis (financial statements, ratios)
- Risk metrics and factor analysis
- Performance attribution

### 3.2 Market Research
- Economic indicators (FRED, IMF, OECD)
- Sector and industry analysis
- News and sentiment (Benzinga, Biztoc)
- Earnings calendars and transcripts

### 3.3 Quantitative Analysis
- Options chains and Greeks
- Technical indicators (via `pandas_ta` integration)
- Backtesting data pipelines
- Factor model construction

### 3.4 AI Agent Integration
- MCP (Model Context Protocol) server for LLM tool use
- Function calling for research assistants
- Automated data retrieval for analysis workflows

---

## 4. Licensing & Pricing

### Open Data Platform (Open Source)

| Attribute | Value |
|-----------|-------|
| **License** | AGPLv3 |
| **Cost** | Free |
| **Deployment** | Local (your environment) |
| **Privacy** | Data stays local |
| **Support** | Community (GitHub, Discord) |

**AGPLv3 Implications:**
- Can use freely for internal research/analysis
- If you modify and expose over a network, must release source
- Suitable for personal use and internal tools
- Commercial products may need legal review

---

## 5. Integration Possibilities

### 5.1 Recommended Approach: SDK Integration

**Why:** Most flexible, aligns with Python/AI focus, full control over data pipelines.

**Configuration:**
```json
// ~/.openbb_platform/user_settings.json
{
  "credentials": {
    "fmp_api_key": "YOUR_KEY",
    "polygon_api_key": "YOUR_KEY",
    "fred_api_key": "YOUR_KEY"
  }
}
```

**Use Cases:**
1. **Daily Research Pipeline** — Automated data pulls for watchlist
2. **Portfolio Monitoring** — Real-time position tracking
3. **Quantitative Models** — Factor data extraction for backtesting
4. **AI Research Agent** — Tool integration for LLM-based analysis

### 5.2 Custom Extensions

OpenBB's architecture allows building custom data provider extensions:
- Add proprietary data sources
- Create custom calculations/metrics
- Build domain-specific routers (e.g., `obb.china.*` for A-share data)

### 5.3 AI Agent (MCP Server)

```bash
pip install openbb-mcp-server
```

Integrate with LLM agents for natural language queries and automated research report generation.

---

## 6. Risks & Considerations

### Technical Risks

| Risk | Mitigation |
|------|------------|
| **API Rate Limits** | Free tiers have limits; plan for caching |
| **Data Quality** | OpenBB doesn't guarantee accuracy (per disclaimer) |
| **Breaking Changes** | Active development may introduce API changes |

### Legal/Compliance

| Risk | Mitigation |
|------|------------|
| **AGPLv3** | If building commercial products, review licensing implications |
| **Data Licensing** | Each provider has own terms; ensure compliance |

### Operational

| Risk | Mitigation |
|------|------------|
| **API Key Management** | Use secure storage (1Password, environment variables) |
| **Data Storage** | Plan for local data persistence and versioning |

---

## 7. Conclusion

OpenBB provides a **production-ready, extensible data infrastructure** for financial research. For a workflow combining deep research, quantitative analysis, and AI agent development, it offers:

- **Immediate value:** 30+ data providers, Python SDK, REST API
- **Long-term flexibility:** Open-source, extensible, no vendor lock-in
- **AI-ready:** MCP server integration for LLM agents

**Recommendation:** Start with Phase 1 (setup & exploration) this week. The learning curve is shallow, and the payoff for research workflows is immediate.

---

## Related

- [[Investment Research Workflow]]
- [[AI Agent Development]]
- [[Financial Data Sources]]
- [[Code as World Model]]

---

*Proposal generated by Finance Agent • 2026-03-14*
