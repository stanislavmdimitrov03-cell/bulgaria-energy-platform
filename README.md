 Bulgaria Energy Intelligence Platform


Overview

An end-to-end data engineering project that analyses the relationship between
weather conditions and Bulgaria's electricity generation mix and wholesale prices.



Core Question

> How does weather (solar radiation, wind speed) relate to Bulgaria's

> electricity generation mix and wholesale electricity prices?



Data Sources

Open-Meteo API — hourly weather data (solar radiation, wind speed, temperature)

ENTSO-E Transparency Platform — day-ahead electricity prices and generation by type



 Tech Stack

Python — data ingestion and transformation

Pandas — data manipulation

DuckDB — local analytical SQL database

Parquet — columnar storage format

Streamlit — interactive dashboard

AWS — S3, Athena, Lambda, EventBridge, SNS



 Project Structure


bulgaria-energy-platform/

├── data/

│   ├── raw/          # original data as downloaded

│   ├── cleaned/      # standardized and validated

│   └── curated/      # final analytical tables

├── notebooks/        # Jupyter exploration notebooks

├── scripts/          # ingestion and transformation scripts

├── dashboard/        # Streamlit dashboard

└── docs/             # project documentation


