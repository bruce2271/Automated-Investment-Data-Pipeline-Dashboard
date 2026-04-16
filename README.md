# Automated Investment Data Pipeline & Dashboard

A personal project built to automate daily collection, storage, and visualization of U.S. and Taiwan market data. The pipeline runs fully unattended every weekday morning via Windows Task Scheduler and Power Automate Desktop.



## Pipeline Overview

```
Python (API collection) → master_investing.xlsx (11 sheets) → Power BI Dashboard
     ↑_____________Power Automate Desktop (daily 06:00 trigger)__________↑
```



## Data Sources

FRED API: U.S. macro indicators — FED funds rate, CPI, PCE, unemployment, nonfarm payrolls, yield spreads
yfinance: Global indices, sector ETFs, U.S. stocks, forex, crypto, VIX
TWSE OpenAPI: Taiwan stock prices, P/E, P/B, dividend yield, 三大法人 institutional flows
SEC EDGAR: Quarterly 13F institutional holdings (Berkshire, BlackRock, Vanguard, State Street, Bridgewater)
Polygon.io: Equity price fallback(backup source)



## Excel Workbook Structure

11 sheets updated daily:

- **Momentum** — macro scorecard with regime signal (Risk-ON / Neutral / Risk-OFF)
- **US Macro** — 24-month FRED time series across rates, inflation, growth, labour, credit
- **Indices** — global index tracker (U.S., Taiwan, Asia, Europe)
- **Signals** — market signals across equities, bonds, commodities, forex, crypto
- **Forex Crypto** — forex rates and crypto prices with daily history columns
- **Sectors** — SPDR sector ETF performance
- **US Stocks** — top holdings by sector with valuation (P/E, P/B, dividend yield)
- **TW Stocks** — Taiwan watchlist with TWSE live data
- **TW Flows** — 三大法人 institutional buy/sell flows, market total and per-stock
- **US 13F** — SEC institutional holdings filtered to watchlist
- **Config** — run log and API configuration reference



## Power BI Dashboard

Built on top of the Excel workbook with DAX measures covering:

- Macro KPI cards (CPI, Core PCE, unemployment, nonfarm payrolls)
- Equity valuation scatter plots (P/E and P/B distribution)
- SEC 13F institutional holdings concentration (donut chart with fund slicer)
- VIX gauge and yield rate bar chart
- Forex rate table with daily history



## Resilience Features

- **FRED cache fallback** — `fred_cache.json` stores last successful fetch; restores any series that returns empty on FRED outage, preventing data loss (e.g. nonfarm payrolls)
- **Duplicate-date guard** — prevents double-writing if the script runs twice in one day
- **yfinance bulk fetch** — single batched download per asset class to avoid rate limiting
- **Multi-source fallback** — yfinance → Stooq → Polygon ETF proxy for indices; yfinance → Polygon for forex and crypto



## Setup

**Requirements**

```bash
pip install requests fredapi pandas openpyxl yfinance pandas-datareader
```

**API Keys** (free tiers sufficient)

```bash
# Windows
setx FRED_API_KEY    "your_key"
setx POLYGON_API_KEY "your_key"
```

- FRED key: https://fred.stlouisfed.org/docs/api/api_key.html
- Polygon key: https://polygon.io

**Run manually**

```bash
python investing_data_collector.py
```

**Schedule (Windows Task Scheduler)**

Set trigger to daily 06:00 Mon–Fri, action to start Power Automate Desktop workflow, which will run `investing_data_collector.py`, after generating excel file, PowerBI service will trigger to refresh data source after 6:30, finally above mentioned workflow will send out email within the attchment of excel file and PBI service link.



## Tech Stack

Python · pandas · openpyxl · yfinance · FRED API · TWSE OpenAPI · SEC EDGAR · Power BI · DAX · Power Automate Desktop · Windows Task Scheduler



## Notes

- Taiwan stocks use TWSE OpenAPI for live OHLC data and yfinance historical closes for the daily history columns
- Historical date columns accumulate one column per trading day on the right side of each sheet — Power BI reads the unpivoted version via Power Query
- `fred_cache.json`, `state.json`, `collector.log`, and the Excel/pbix files are excluded from this repo via `.gitignore`
