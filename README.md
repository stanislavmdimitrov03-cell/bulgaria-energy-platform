# Bulgaria Energy Intelligence Platform

I built this project to explore a question that genuinely interested me —
does the weather actually affect electricity prices in Bulgaria?

Turns out, it does. Solar generation peaks at midday and prices drop by 
68%. At night when solar disappears, prices spike. Simple idea, but 
seeing it in real market data made it click.

---

## What This Project Does

Pulls real electricity and weather data for Bulgaria, transforms it, 
and visualizes the relationships through a dashboard.

Data sources:
- Weather data for 5 Bulgarian cities from Open-Meteo (free API)
- Day-ahead electricity prices from ENTSO-E (real market data)
- Hourly generation by source from ENTSO-E (solar, wind, nuclear, coal...)

The pipeline runs automatically on AWS every day and the dashboard 
connects to live cloud data.

---

## What I Found

- The cheapest hour of the day is 13:00 (61.59 EUR/MWh) — solar peak
- The most expensive is 20:00 (195.32 EUR/MWh) — solar gone, people home
- Solar radiation and solar generation correlate at 0.865 — strong
- Bulgaria had 55 hours of negative electricity prices in 2024
- Nuclear power provides 43% of Bulgaria's electricity and never stops

---

## Tech Stack

 Data fetching | Python, requests, entsoe-py |
 Local storage | Parquet, DuckDB |
 Cloud storage | AWS S3 |
 Cloud queries | AWS Athena |
 Automation | AWS Lambda, EventBridge |
 Alerting | AWS SNS |
 Dashboard | Streamlit 

---

## Project Structure

bulgaria-energy-platform/
│
├── data/
│   ├── raw/          
│   ├── cleaned/      
│   └── curated/      
│
├── scripts/          # data fetching and transformation
├── notebooks/        # exploration and analysis
├── dashboard/        # Streamlit app
└── docs/             # documentation


---

## How to Run It
```bash
git clone https://github.com/stanislavmdimitrov03-cell/bulgaria-energy-platform.git
cd bulgaria-energy-platform

python -m venv .venv
.venv\Scripts\activate

pip install requests pandas duckdb jupyter matplotlib pyarrow entsoe-py python-dotenv streamlit pyathena boto3

# Add your ENTSO-E API key to .env
echo "ENTSOE_API_KEY=your_key_here" > .env

# Fetch the data
python scripts/fetch_weather_all_cities.py
python scripts/fetch_entsoe_prices.py
python scripts/fetch_entsoe_generation.py

# Build the database
python scripts/setup_duckdb.py
python scripts/create_cleaned_tables.py
python scripts/create_star_schema.py

# Run the dashboard
streamlit run dashboard/app.py

---

## Dashboard

Four pages:
- Energy mix overview — how Bulgaria generated electricity month by month
- Price analysis — when prices are high, low, and why
- Weather and renewables — does sunshine actually produce cheaper electricity?
- Key insights — summary of the main findings

---

## Docs

- [Full project documentation](docs/project_documentation.md)
- [Technical decisions and challenges](docs/decisions_and_challenges.md)

---

## About

Built by Stanislav Dimitrov as a personal data engineering project.  

