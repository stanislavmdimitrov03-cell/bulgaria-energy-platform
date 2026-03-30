# =============================================================================
# create_cleaned_tables.py
# Creates cleaned versions of our raw tables in DuckDB
# Adds derived columns, standardizes types, handles nulls
# =============================================================================

import duckdb

# Connect to our existing database
import os
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'bulgaria_energy.db')
con = duckdb.connect(db_path)
print("Connected to DuckDB database")

# -----------------------------------------------------------------------------
# STEP 1: Create cleaned weather table
# We add derived time columns that will be very useful for analysis
# -----------------------------------------------------------------------------

print("Creating cleaned weather table...")

con.execute("""
    CREATE OR REPLACE TABLE cleaned_weather AS
    SELECT
        -- Original columns, kept as-is
        timestamp,
        city,
        latitude,
        longitude,
        shortwave_radiation_wm2,
        windspeed_10m_kmh,
        temperature_2m_c,

        -- Derived time columns (extracted from timestamp)
        -- These make grouping and filtering much easier in analysis
        YEAR(timestamp)                    AS year,
        MONTH(timestamp)                   AS month,
        DAY(timestamp)                     AS day,
        HOUR(timestamp)                    AS hour,
        DAYOFWEEK(timestamp)               AS day_of_week,  -- 0=Sunday, 6=Saturday
        DAYOFYEAR(timestamp)               AS day_of_year,

        -- Season column based on month
        -- Useful for seasonal analysis without complex date logic later
        CASE
            WHEN MONTH(timestamp) IN (12, 1, 2)  THEN 'Winter'
            WHEN MONTH(timestamp) IN (3, 4, 5)   THEN 'Spring'
            WHEN MONTH(timestamp) IN (6, 7, 8)   THEN 'Summer'
            WHEN MONTH(timestamp) IN (9, 10, 11) THEN 'Autumn'
        END AS season,

        -- Is this a daytime hour? (solar radiation > 0 means sun is up)
        CASE
            WHEN shortwave_radiation_wm2 > 0 THEN TRUE
            ELSE FALSE
        END AS is_daytime,

        -- Radiation category for easy grouping
        CASE
            WHEN shortwave_radiation_wm2 = 0    THEN 'None'
            WHEN shortwave_radiation_wm2 < 100  THEN 'Low'
            WHEN shortwave_radiation_wm2 < 400  THEN 'Medium'
            WHEN shortwave_radiation_wm2 < 700  THEN 'High'
            ELSE 'Very High'
        END AS radiation_category

    FROM raw_weather

    -- Filter out any rows where timestamp is null (data quality check)
    WHERE timestamp IS NOT NULL
""")

# Verify the cleaned table
row_count = con.execute("SELECT COUNT(*) FROM cleaned_weather").fetchone()[0]
print(f"  ✓ cleaned_weather table created: {row_count} rows")

# -----------------------------------------------------------------------------
# STEP 2: Preview the cleaned table
# -----------------------------------------------------------------------------

print("\nSample of cleaned_weather table:")
result = con.execute("""
    SELECT
        timestamp, city, shortwave_radiation_wm2,
        hour, month, season, is_daytime, radiation_category
    FROM cleaned_weather
    WHERE shortwave_radiation_wm2 > 0  -- show only daytime rows
    LIMIT 5
""").df()
print(result)

# -----------------------------------------------------------------------------
# STEP 3: Run a meaningful analytical query on the cleaned data
# Average solar radiation by season and city
# -----------------------------------------------------------------------------

print("\nAverage solar radiation by season:")
result = con.execute("""
    SELECT
        season,
        ROUND(AVG(shortwave_radiation_wm2), 1) AS avg_radiation,
        ROUND(AVG(temperature_2m_c), 1)        AS avg_temperature
    FROM cleaned_weather
    GROUP BY season
    ORDER BY avg_radiation DESC
""").df()
print(result)

con.close()
print("\nDone!")
