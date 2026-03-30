# =============================================================================
# setup_duckdb.py
# Creates a DuckDB database and loads our raw Parquet files into tables
# Think of this as "setting up our local data warehouse"
# =============================================================================

import duckdb  # the database library
import os      # for file path operations

# -----------------------------------------------------------------------------
# STEP 1: Create (or connect to) the DuckDB database file
# If the file doesn't exist, DuckDB creates it automatically
# If it already exists, DuckDB just opens it
# -----------------------------------------------------------------------------

# This creates a file called "bulgaria_energy.db" in your project root
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'bulgaria_energy.db')
con = duckdb.connect(db_path)  # con = connection, our gateway to the database

print(f"Connected to DuckDB database: {db_path}")

# -----------------------------------------------------------------------------
# STEP 2: Load the weather data into a DuckDB table
# DuckDB can read Parquet files directly with read_parquet()
# We wrap it in CREATE OR REPLACE TABLE to make it a permanent table
# -----------------------------------------------------------------------------

print("Loading weather data...")

parquet_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
               '..', 'data', 'raw', 'weather_all_cities_2024.parquet')

con.execute(f"""
    CREATE OR REPLACE TABLE raw_weather AS
    SELECT * FROM read_parquet('{parquet_path}')
""")

# Verify it loaded correctly by counting rows
row_count = con.execute("SELECT COUNT(*) FROM raw_weather").fetchone()[0]
print(f"  ✓ raw_weather table created: {row_count} rows")

# -----------------------------------------------------------------------------
# STEP 3: Preview the data with SQL
# -----------------------------------------------------------------------------

print("\nFirst 5 rows of raw_weather:")
result = con.execute("""
    SELECT *
    FROM raw_weather
    LIMIT 5
""").df()  # .df() converts the result into a pandas DataFrame for display

print(result)

# -----------------------------------------------------------------------------
# STEP 4: Run a quick analytical query to verify everything works
# This query shows us the average solar radiation per city
# -----------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------
# STEP 5: Close the connection cleanly
# Always close your database connection when you're done
# -----------------------------------------------------------------------------

con.close()
print(f"\nDatabase saved to: {db_path}")
print("Done!")