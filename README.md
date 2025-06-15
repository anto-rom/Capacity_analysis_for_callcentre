📊 Call Center Capacity Forecasting
Structured script to create a predictive forecast model for capacity calculation for Q3-Q4 of 2025
# 📊 Call Center Capacity Forecasting

This repository contains a Python-based analysis for forecasting workload and estimating staffing needs in a multilingual call center environment.

## 🚀 Project Overview

This project aims to:
- Analyze historical call center data (calls + tickets).
- Forecast daily volume by language and queue using SARIMAX models.
- Simulate agent staffing needs to achieve specific service-level targets.
- Export actionable planning outputs in Excel format.

## 📁 Project Structure

- `final_capacity_with_forecast.ipynb`: Complete Jupyter Notebook with analysis, forecasting, and simulation logic.
- `forecast_and_capacity.xlsx`: Excel file with forecasted item volumes and required agent capacity.
- `final_capacity_with_forecast.html`: Web-ready HTML version of the notebook for sharing/presentations.
- `README.md`: Project documentation.

## 📈 Forecasting Logic

The notebook applies a time series model (`SARIMAX`) to predict future item volumes until **December 31st, 2025**. Forecasts are generated per:
- **Language**
- **Queue**
- **Day**

## 👨‍💻 Agent Simulation

Simulations estimate the number of agents required per day to handle volume within 2 days, targeting:
- ✅ 80% resolution
- ✅ 85% resolution
- ✅ 90% resolution

Assumption: each agent handles **15 items/day**.

## 📦 Outputs

The final Excel file includes:
- Sheet `Forecast`: Predicted number of items per day/language/queue.
- Sheet `Agents_Needed`: Estimated number of agents to meet service goals.

## 🔧 Requirements

This project was built using:
- Python 3.9+
- `pandas`, `numpy`
- `matplotlib`, `seaborn`
- `statsmodels`
- `openpyxl`

Install dependencies:
```bash
pip install -r requirements.txt
🧠 Author
Antonio Romero
Continuous Improvement & VoC Leader
📧 antonio.33a61@gmail.com
📍 Madrid, Spain

