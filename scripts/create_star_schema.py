

import duckdb
import os

# Connect to our existing database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bulgaria_energy.db'))
con = duckdb.connect(db_path)
con.execute("SET temp_directory = 'C:/Users/stann/Projects/bulgaria-energy-platform/tmp'")

print("Connected to DuckDB database")


# STEP 1: Create dim_date


print("Creating dim_date...")

con.execute("""
    CREATE OR REPLACE TABLE dim_date AS
    SELECT DISTINCT
        
        CAST(STRFTIME(timestamp, '%Y%m%d%H') AS INTEGER) AS date_key,

        timestamp,
        YEAR(timestamp)                                   AS year,
        MONTH(timestamp)                                  AS month,
        DAY(timestamp)                                    AS day,
        HOUR(timestamp)                                   AS hour,
        DAYOFWEEK(timestamp)                              AS day_of_week,
        DAYOFYEAR(timestamp)                              AS day_of_year,

        
        STRFTIME(timestamp, '%B')                         AS month_name,

       
        STRFTIME(timestamp, '%A')                         AS day_name,

        
        CASE
            WHEN DAYOFWEEK(timestamp) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        
        CASE
            WHEN MONTH(timestamp) IN (12, 1, 2)  THEN 'Winter'
            WHEN MONTH(timestamp) IN (3, 4, 5)   THEN 'Spring'
            WHEN MONTH(timestamp) IN (6, 7, 8)   THEN 'Summer'
            WHEN MONTH(timestamp) IN (9, 10, 11) THEN 'Autumn'
        END AS season,

        
        CASE
            WHEN HOUR(timestamp) BETWEEN 6 AND 21 THEN TRUE
            ELSE FALSE
        END AS is_business_hours

    FROM cleaned_weather
    ORDER BY timestamp
""")

count = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
print(f"   dim_date created: {count} rows")


# STEP 2: Create dim_city


print("Creating dim_city...")

con.execute("""
    CREATE OR REPLACE TABLE dim_city AS
    WITH distinct_cities AS (
        SELECT DISTINCT city, latitude, longitude
        FROM cleaned_weather
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY city) AS city_key,
        city,
        latitude,
        longitude,
        CASE city
            WHEN 'Sofia'   THEN 'Southwest'
            WHEN 'Plovdiv' THEN 'South'
            WHEN 'Varna'   THEN 'Northeast'
            WHEN 'Burgas'  THEN 'Southeast'
            WHEN 'Pleven'  THEN 'North'
        END AS region,
        CASE city
            WHEN 'Varna'  THEN TRUE
            WHEN 'Burgas' THEN TRUE
            ELSE FALSE
        END AS is_coastal
    FROM distinct_cities
    ORDER BY city
""")

count = con.execute("SELECT COUNT(*) FROM dim_city").fetchone()[0]
print(f"   dim_city created: {count} rows")
print(con.execute("SELECT * FROM dim_city").df())


print("\ndim_city contents:")
print(con.execute("SELECT * FROM dim_city").df())

# STEP 3: Create fct_weather_hourly


print("\nCreating fct_weather_hourly...")

con.execute("""
    CREATE OR REPLACE TABLE fct_weather_hourly AS
    SELECT
        -- Foreign keys linking to dimension tables
        CAST(STRFTIME(w.timestamp, '%Y%m%d%H') AS INTEGER) AS date_key,
        c.city_key,

        -- The actual measurements (facts)
        w.shortwave_radiation_wm2,
        w.windspeed_10m_kmh,
        w.temperature_2m_c,

        -- Derived measurement columns
        w.is_daytime,
        w.radiation_category

    FROM cleaned_weather w
    -- Join to dim_city to get the city_key foreign key
    JOIN dim_city c ON w.city = c.city

    ORDER BY w.timestamp, w.city
""")

count = con.execute("SELECT COUNT(*) FROM fct_weather_hourly").fetchone()[0]
print(f"   fct_weather_hourly created: {count} rows")


# STEP 4: Test the star schema with an analytical query


print("\nTest query — average summer solar radiation by city and region:")
result = con.execute("""
    SELECT
        c.city,
        c.region,
        c.is_coastal,
        ROUND(AVG(f.shortwave_radiation_wm2), 1) AS avg_solar_radiation,
        ROUND(AVG(f.temperature_2m_c), 1)        AS avg_temperature
    FROM fct_weather_hourly f
    JOIN dim_city c ON f.city_key = c.city_key
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE d.season = 'Summer'
    GROUP BY c.city, c.region, c.is_coastal
    ORDER BY avg_solar_radiation DESC
""").df()

print(result)

# STEP 4: Create fct_prices_hourly


print("Creating fct_prices_hourly...")

con.execute("""
    CREATE OR REPLACE TABLE fct_prices_hourly AS
    SELECT
        -- Foreign key to dim_date
        CAST(STRFTIME(timestamp, '%Y%m%d%H') AS INTEGER) AS date_key,

        -- Price measurements
        price_eur_mwh,
        price_category,
        is_negative_price,
        season,
        hour

    FROM cleaned_prices
    ORDER BY timestamp
""")

count = con.execute("SELECT COUNT(*) FROM fct_prices_hourly").fetchone()[0]
print(f"   fct_prices_hourly created: {count} rows")


# STEP 5: Create fct_generation_hourly


print("Creating fct_generation_hourly...")

con.execute("""
    CREATE OR REPLACE TABLE fct_generation_hourly AS
    SELECT
        -- Foreign key to dim_date
        CAST(STRFTIME(timestamp, '%Y%m%d%H') AS INTEGER) AS date_key,

        -- Individual source measurements (MW)
        nuclear_mw,
        lignite_mw,
        solar_mw,
        gas_mw,
        hydro_reservoir_mw,
        wind_onshore_mw,
        hydro_river_mw,
        hard_coal_mw,
        biomass_mw,
        waste_mw,
        hydro_pumped_mw,

        -- Derived aggregate columns
        total_generation_mw,
        renewable_mw,
        fossil_mw,

        -- Renewable share percentage
        ROUND(renewable_mw / NULLIF(total_generation_mw, 0) * 100, 2) AS renewable_pct,

        -- Season and time
        season,
        hour

    FROM cleaned_generation
    ORDER BY timestamp
""")

count = con.execute("SELECT COUNT(*) FROM fct_generation_hourly").fetchone()[0]
print(f"   fct_generation_hourly created: {count} rows")


# STEP 6: The query — join all tables together


print("\nCore analytical query — weather vs prices vs generation:")
result = con.execute("""
    WITH sofia_weather AS (
        SELECT date_key,
               shortwave_radiation_wm2,
               windspeed_10m_kmh,
               temperature_2m_c
        FROM fct_weather_hourly
        WHERE city_key = 4
    )
    SELECT
        d.season,
        ROUND(AVG(w.shortwave_radiation_wm2), 1) AS avg_solar_radiation,
        ROUND(AVG(g.solar_mw), 1)                AS avg_solar_generation_mw,
        ROUND(AVG(g.renewable_pct), 1)           AS avg_renewable_pct,
        ROUND(AVG(p.price_eur_mwh), 2)           AS avg_price_eur_mwh
    FROM fct_generation_hourly g
    JOIN fct_prices_hourly p ON g.date_key = p.date_key
    JOIN dim_date d          ON g.date_key = d.date_key
    JOIN sofia_weather w     ON g.date_key = w.date_key
    GROUP BY d.season
    ORDER BY avg_solar_radiation DESC
""").df()

print(result)


con.close()
print("\nStar schema complete!")
