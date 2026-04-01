# Project Documentation

This document covers the technical details of the Bulgaria Energy 
Intelligence Platform — how it was built, what data it uses, and 
how the pieces fit together.

---

## Data Sources

### Open-Meteo
Free weather API, no registration needed. I used the historical 
archive endpoint to pull a full year of hourly data for 5 cities.

Variables collected:
- Solar radiation — measures how much sunlight hits the ground
- Wind speed at 10m 
- Temperature at 2m

Cities: Sofia, Plovdiv, Varna, Burgas, Pleven  
Period: 2024 full year  
Rows: 43,920 (5 cities × 8,784 hours)  
File: `data/raw/weather_all_cities_2024.parquet`

### ENTSO-E — Day-Ahead Prices
The European electricity market transparency platform. Requires a 
free API key and email request to activate it.

Pulls the wholesale day-ahead price for Bulgaria in EUR/MWh — 
the price set the day before for each hour of the following day.

Rows: 8,761  
File: `data/raw/entsoe_prices_bulgaria_2024.parquet`

### ENTSO-E — Generation by Type
Same platform, different endpoint. Returns how many MW each 
generation source produced each hour.

Sources: Nuclear, Lignite, Solar, Wind, Hydro (reservoir + river + 
pumped), Gas, Hard Coal, Biomass, Waste  
Rows: 8,760  
File: `data/raw/entsoe_generation_bulgaria_2024.parquet`

---

## Data Model

I modeled the data as a star schema — a standard pattern in data 
warehousing where one large fact table sits in the middle and 
smaller dimension tables surround it.

### Why a star schema?
It avoids repeating city names and timestamps thousands of times, 
makes SQL queries cleaner, and mirrors how production data warehouses 
are actually built.

### Dimension Tables

**dim_date** — one row per unique hour  
Contains year, month, day, hour, season, day name, weekend flag.  
Primary key: `date_key` (format YYYYMMDDHH)

**dim_city** — one row per city  
Contains city name, coordinates, region, coastal flag.  
Primary key: `city_key`

### Fact Tables

**fct_weather_hourly** — 43,920 rows  
One row per city per hour. Stores radiation, wind speed, temperature.  
Foreign keys: `date_key`, `city_key`

**fct_prices_hourly** — 8,761 rows  
One row per hour. Stores price, price category, negative price flag.  
Foreign key: `date_key`

**fct_generation_hourly** — 8,760 rows  
One row per hour. Stores MW per source, totals, renewable percentage.  
Foreign key: `date_key`

---

## Pipeline

### Local pipeline
```
APIs → Python scripts -> Parquet files -> DuckDB -> Streamlit
```

### Cloud pipeline
```
EventBridge (06:00 UTC) -> Lambda -> APIs -> S3 ->Glue -> Athena -> Streamlit
```

The dashboard supports both with a single toggle:
```python
USE_CLOUD = False  # True for Athena, False for local DuckDB
```

---

## Cloud Infrastructure



| S3 | Stores all Parquet files in raw/cleaned/curated zones |
| Glue | Catalogs the S3 files so Athena knows they exist |
| Athena | Runs SQL queries directly against S3 files |
| Lambda | Runs the weather fetch script daily |
| EventBridge | Triggers Lambda at 06:00 UTC every day |
| SNS | Sends email alert if Lambda fails |
| IAM | Controls permissions — separate user from root account |

---

## Data Quality Notes

**Negative prices (55 hours):** Legitimate market events when 
renewable generation exceeds demand. Retained and flagged.

**Price spike to 950 EUR/MWh:** Verified as real — supply crisis 
moment. Retained.

**Row count difference:** Prices have 8,761 rows vs 8,760 for 
generation. Likely a daylight saving time hour. Immaterial.

**Weak wind correlation (0.177):** Not a data error. Sofia wind 
speed is a poor proxy for national wind generation because 
Bulgaria's wind farms are on the Black Sea coast, not inland.