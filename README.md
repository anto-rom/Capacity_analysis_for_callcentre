
# Capacity & Language Staffing Forecast – README

This repository contains a production-ready Python script (`prophet_forecast_v4.py`) to forecast daily workload, plan capacity scenarios, and compute language staffing requirements. It uses Facebook Prophet (now `prophet`) for time-series forecasting, includes robust weekday-specific RR% estimation, shrinkage-aware capacity scenarios, and generates two Excel workbooks with charts and a scenario picker.
---

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Input Data](#input-data)
- [Outputs](#outputs)
- [How It Works (Pipeline)](#how-it-works-pipeline)
  - [RR% by Weekday (Trend-Aware)](#rr-by-weekday-trend-aware)
  - [Weekend Factors](#weekend-factors)
  - [Forecast Uplift Rule](#forecast-uplift-rule)
  - [Capacity & Backlog](#capacity--backlog)
  - [Language Staffing](#language-staffing)
  - [Thu–Wed Week Logic](#thuwed-week-logic)
- [Configuration](#configuration)
- [Installation](#installation)
- [Running the Script](#running-the-script)
- [Charts](#charts)
- [Troubleshooting](#troubleshooting)
- [Customization Tips](#customization-tips)
- [License](#license)

---

## Overview
`prophet_forecast_v4.py` forecasts **Net Emails** and converts them into **Total Workload** by applying a robust, weekday-specific **RR%** estimate. It adjusts for weekend patterns, computes multiple weekday staffing scenarios (18/20/24) with a fixed weekend staffing, incorporates **shrinkage** as lost-agent equivalents, and generates:

- A **main workbook** with forecast, audit, and metadata.
- A **language staffing workbook** with daily and weekly (Thu–Wed) staffing by language, a Scenario Picker, comparisons, and charts.

## Features
- **Prophet-based forecast** on `Net_Emails` with yearly and weekly seasonality.
- **RR% by weekday (trend-aware):** EWM per weekday sub-series with robust fallbacks.
- **Weekend factor** estimation from history with bounds and fallbacks.
- **Shrinkage-aware capacity** and rolling backlog under multiple weekday staffing scenarios.
- **Language staffing** via percentage split and **Largest Remainders** apportionment.
- **Thu–Wed week** aggregation to match operational reporting.
- **Excel outputs** (OpenPyXL) with embedded **PNG charts** and **data validation** in Scenario Picker.

## Input Data
### 1) Items per Day (Excel)
**Path (default):** `C:/Users/pt3canro/Desktop/CAPACITY/items_per_day.xlsx`

Required columns:
- `Date` (date)
- `Emails` (number)
- `Spam` (number)
- `Outage` (number)
- `RR` (percentage, e.g., 12.3 for 12.3%)

Derived in-script:
- `Net_Emails = max(0, Emails - Spam - Outage)`
- `Total_Workload_real = Net_Emails * (1 + RR/100)`
- `Weekday = day_name(Date)`

### 2) Historical Shrinkage (Excel)
**Path (default):** `C:/Users/pt3canro/Desktop/CAPACITY/Historical Shrinkage.xlsx`

Accepted columns (any one naming convention):
- `Date` / `date` / `FECHA`
- `shrinkage_hours` or `shrinkage hours`
- **or** `shrinkage_seconds` / `shrinkage seconds` (auto-converted to hours)

The script computes **median shrinkage hours by weekday** and converts them into **lost agent equivalents**: `lost_agents = shrinkage_hours / 8`.

> If the shrinkage file is missing, shrinkage defaults to **0**.

## Outputs
### A) Main Workbook
**Path:** `C:/Users/pt3canro/Desktop/CAPACITY/OUTPUTS/future_preditions.xlsx`

Sheets:
- **Forecast** — Daily forecast with `yhat_net`, weekend factor, `yhat_net_adj`, `RR_hat`, `Total_Workload_forecast`, shrinkage estimates, capacity and cumulative backlog for scenarios **WD18/WD20/WD24**.
- **Audit** — Historical alignment: real vs predicted workload, errors and MAPE.
- **Meta** — Configuration snapshot (cases per agent, weekend agents, options, weekend factors, RR_hat per weekday, shrinkage medians). Also embeds charts.

### B) Language Staffing Workbook
**Path:** `C:/Users/pt3canro/Desktop/CAPACITY/OUTPUTS/language_staffing_requirements.xlsx`

Sheets:
- **Daily_Language_Staffing** — Daily workload base, per-language split, and required agents per language (ceil).
- **Weekly_ThuWed_Summary** — Thu–Wed aggregation of the above, with agents per language.
- **Scenario_Picker** — Column `Scenario_WD` for Mon–Fri (data validation: `18,20,24`); weekends fixed at 7.
- **Daily_Comparison** — Required vs planned agents per language per day (using Largest Remainders for apportionment).
- **Weekly_Comparison** — Thu–Wed aggregation of required vs planned.
- **Meta** — Language shares, base used, and sanity notes.
- **Sanity** — Sanity check for the next forecast date (if available).

## How It Works (Pipeline)
1. **Load & preprocess** items and shrinkage.
2. **Fit Prophet** on `Net_Emails` and produce daily `yhat`.
3. **Compute weekend factors** from history (`Saturday`, `Sunday`) with bounds and fallbacks; weekdays fixed to 1.0.
4. **Adjust** `yhat_net` by weekend factor → `yhat_net_adj`.
5. **Estimate RR% by weekday (trend-aware)** and compute `Total_Workload` for history and future.
6. **Apply uplift rule** (+7% up to 2025-09-30) to `Total_Workload_forecast`.
7. **Estimate shrinkage** (median hours by weekday) and convert to lost agents.
8. **Scenario capacity & backlog** for WD18/WD20/WD24; weekends fixed at 7.
9. **Export** main workbook + embed charts.
10. **Language staffing** (daily & weekly Thu–Wed) with **Largest Remainders** apportionment for planned agents; export workbook + charts + validation.

### RR% by Weekday (Trend-Aware)
- For each weekday, compute an **EWM** (exponential weighted mean) on the **sub-series of that weekday** only (e.g., Mondays → only Mondays), using a **~4-week halflife**.
- If insufficient data for a weekday, fall back to:
  `RR_hat(dow) = RR_level_global_EWM * median(dow)/median(global)`.
- Guarantees a **non-negative** RR% and returns a value for all 7 weekdays.

> The current weekly halflife (≈4) is defined inside `_rr_dynamic_by_dow`. You can move it to `Config` if you want it configurable.

### Weekend Factors
- Computed by comparing `Net_Emails` vs `yhat_net` on **`Saturday`** and **`Sunday`**.
- Uses **median ratio** when ≥4 points exist; otherwise **fallback** is used.
- Enforced within **bounds**: `Saturday` in `[0.40, 0.90]`, `Sunday` in `[0.40, 0.90]`.

### Forecast Uplift Rule
- Business rule: **+7% uplift** applied to `Total_Workload_forecast` **up to** `2025-09-30`.
- Controlled in code: see `cutoff_date = pd.Timestamp("2025-09-30")` and the subsequent mask.

### Capacity & Backlog
- Converts **effective agents** (planned minus lost-agent equivalents) to **daily capacity**: `capacity = effective_agents * CASES_PER_AGENT`.
- **Backlog** is a simple cumulative carry-over of `(Total_Workload_forecast - capacity)` with non-negativity.
- Scenarios provided for weekday staffing: **18, 20, 24** (weekends fixed at **7**).

### Language Staffing
- **Split** daily workload by configured language shares.
- Compute **Agents_lang = ceil(workload_lang / CASES_PER_AGENT)**, daily and Thu–Wed weekly.
- **Scenario Picker:** choose weekday agents (18/20/24). Apportion daily planned agents to languages using **Largest Remainders**.
- **Comparisons:** Required vs planned, daily and weekly (Thu–Wed).

### Thu–Wed Week Logic
- Weeks run **Thursday → Wednesday**. The **week start** is the **Thursday** of that week.
- Implemented by `_antonio_week_start(...)`.

## Configuration
Open `prophet_forecast_v4.py` and edit the `Config` dataclass:

```python
@dataclass
class Config:
    PATH_ITEMS: str = r"C:/Users/pt3canro/Desktop/CAPACITY/items_per_day.xlsx"
    PATH_SHRINK: str = r"C:/Users/pt3canro/Desktop/CAPACITY/Historical Shrinkage.xlsx"
    OUTPUT_XLSX: str = r"C:/Users/pt3canro/Desktop/CAPACITY/OUTPUTS/future_preditions.xlsx"
    OUTPUT_LANG_XLSX: str = r"C:/Users/pt3canro/Desktop/CAPACITY/OUTPUTS/language_staffing_requirements.xlsx"
    HORIZON_DAYS: int = 365
    CASES_PER_AGENT: int = 18
    WEEKEND_AGENTS: int = 7
    WEEKDAY_AGENT_OPTIONS: Tuple[int, int, int] = (18, 20, 24)
    LOOKBACK_MIN_DAYS: int = 112
    # Weekend bounds & Prophet settings omitted for brevity
    LANG_SPLIT_PCT: Dict[str, float] = { ... }
    LANG_BASE: str = 'total'  # or 'net'
```

- **Language split** must sum ~100% (auto-normalized if slightly off).
- To change **RR weekly halflife**, edit `weekly_halflife` inside `_rr_dynamic_by_dow`.

## Installation
**Python:** 3.9–3.11 recommended.

Install dependencies:
```bash
pip install --upgrade pip
pip install prophet pandas numpy openpyxl matplotlib seaborn
```
> On some systems, `prophet` may require a working C++ toolchain; wheels are available for common Python versions. If you face issues, consider installing `cmdstanpy` first or consult Prophet installation docs.

## Running the Script
From the project folder:
```bash
python prophet_forecast_v4.py
```
Results will be written under `C:/Users/pt3canro/Desktop/CAPACITY/OUTPUTS/` (by default). Ensure the input file paths are correct and accessible.

## Charts
The script saves PNGs alongside the output Excels and embeds them:
- `avg_total_workload_by_weekday.png`
- `avg_real_vs_pred_by_weekday.png`
- `daily_trend_real_vs_pred.png`
- `lang_agents_next30.png`
- `lang_agents_weekly.png`

## Troubleshooting
- **IndentationError at the end:** ensure you have
  ```python
  if __name__ == '__main__':
      main()
  ```
- **FileNotFoundError:** check `PATH_ITEMS` and `PATH_SHRINK` in `Config`.
- **Missing columns:** verify the input files have the required column names.
- **Prophet installation issues:** upgrade `pip`, ensure compatible Python version, and try reinstalling `prophet`.
- **Charts not embedded:** images are saved, but Excel embedding can fail if the file is open; close the workbook and rerun.

## Customization Tips
- Make `weekly_halflife` a `Config` parameter if you want to tune weekday RR sensitivity.
- Add more weekday staffing **scenarios** by extending `WEEKDAY_AGENT_OPTIONS`.
- Change **LANG_BASE** to `'net'` if you want staffing based on `yhat_net_adj` instead of total workload.
- Modify the **uplift rule** as needed (amount and cutoff date).

## License
Internal use. If you plan to distribute this code externally, add an appropriate open-source or proprietary license file.



Install dependencies:
```bash
pip install -r requirements.txt
🧠 Author
Antonio Romero
Continuous Improvement & VoC Leader
📧 antonio.33a61@gmail.com
📍 Madrid, Spain

