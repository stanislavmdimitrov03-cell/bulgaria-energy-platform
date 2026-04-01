

import duckdb
import os

# STEP 1: Create (or connect to) the DuckDB database file


# creates a file called "bulgaria_energy.db" project root
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'bulgaria_energy.db')
con = duckdb.connect(db_path)
print(f"Connected to DuckDB database: {db_path}")

# STEP 2: Load the weather data into a DuckDB table


print("Loading weather data...")

parquet_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
               '..', 'data', 'raw', 'weather_all_cities_2024.parquet')

con.execute(f"""
    CREATE OR REPLACE TABLE raw_weather AS
    SELECT * FROM read_parquet('{parquet_path}')
""")

# Verify it loaded correctly by counting rows
row_count = con.execute("SELECT COUNT(*) FROM raw_weather").fetchone()[0]
print(f"   raw_weather table created: {row_count} rows")

# STEP 3: Preview the data with SQL


print("\nFirst 5 rows of raw_weather:")
result = con.execute("""
    SELECT *
    FROM raw_weather
    LIMIT 5
""").df()

print(result)
# STEP 4: Run a quick analytical query to verify everything works

print("\nAverage solar radiation by city:")
result = con.execute("""
    SELECT
        city,
        ROUND(AVG(shortwave_radiation_wm2), 2) AS avg_solar_radiation,
        ROUND(MAX(shortwave_radiation_wm2), 2) AS max_solar_radiation,
        COUNT(*) AS total_hours
    FROM raw_weather
    GROUP BY city
    ORDER BY avg_solar_radiation DESC
""").df()

print(result)

# STEP 5: Close the connection cleanly


print("Loading electricity prices data...")

prices_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
              '..', 'data', 'raw', 'entsoe_prices_bulgaria_2024.parquet'))

con.execute(f"""
    CREATE OR REPLACE TABLE raw_prices AS
    SELECT * FROM read_parquet('{prices_path}')
""")

row_count = con.execute("SELECT COUNT(*) FROM raw_prices").fetchone()[0]
print(f"   raw_prices table created: {row_count} rows")

# Load ENTSO-E generation into DuckDB


print("Loading electricity generation data...")

gen_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
           '..', 'data', 'raw', 'entsoe_generation_bulgaria_2024.parquet'))

con.execute(f"""
    CREATE OR REPLACE TABLE raw_generation AS
    SELECT * FROM read_parquet('{gen_path}')
""")

row_count = con.execute("SELECT COUNT(*) FROM raw_generation").fetchone()[0]
print(f"   raw_generation table created: {row_count} rows")

con.close()
print(f"\nDatabase saved to: {db_path}")
print("Done!")