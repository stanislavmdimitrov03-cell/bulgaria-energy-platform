# =============================================================================
# fetch_entsoe_prices.py
# Pulls day-ahead electricity prices for Bulgaria for full year 2024
# from ENTSO-E Transparency Platform and saves as Parquet
# =============================================================================

import pandas as pd
import os
from entsoe import EntsoePandasClient
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# STEP 1: Load API key and create client
# -----------------------------------------------------------------------------

# Load the API key from our .env file
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path)
api_key = os.getenv("ENTSOE_API_KEY")

if not api_key:
    raise ValueError("ENTSOE_API_KEY not found in .env file")

# Create the ENTSO-E client — this handles all API communication for us
client = EntsoePandasClient(api_key=api_key)
print("ENTSO-E client created successfully")

# -----------------------------------------------------------------------------
# STEP 2: Define the time range
# We pull the full year 2024 in one request
# Timezone must be specified — ENTSO-E works in local time
# -----------------------------------------------------------------------------

start = pd.Timestamp('20240101', tz='Europe/Sofia')
end   = pd.Timestamp('20241231', tz='Europe/Sofia')

print(f"Fetching day-ahead prices for Bulgaria: {start.date()} to {end.date()}")

# -----------------------------------------------------------------------------
# STEP 3: Fetch the data
# query_day_ahead_prices returns a pandas Series with timestamps as index
# and prices in €/MWh as values
# -----------------------------------------------------------------------------

prices = client.query_day_ahead_prices('BG', start=start, end=end)

print(f"Data received: {len(prices)} hourly records")

# -----------------------------------------------------------------------------
# STEP 4: Convert to a clean DataFrame
# -----------------------------------------------------------------------------

# Convert the Series to a DataFrame
df = pd.DataFrame({
    'timestamp': prices.index,      # the datetime index becomes a column
    'price_eur_mwh': prices.values  # the price values
})

# Remove timezone info from timestamp and convert to naive datetime
# We already know it's Sofia time, storing tz info causes issues with Parquet
df['timestamp'] = df['timestamp'].dt.tz_localize(None)

# Add metadata columns
df['country'] = 'Bulgaria'
df['currency'] = 'EUR'
df['unit'] = 'MWh'

# Basic data exploration
print(f"\nDataFrame shape: {df.shape}")
print(f"\nBasic statistics:")
print(df['price_eur_mwh'].describe().round(2))
print(f"\nFirst 5 rows:")
print(df.head())
print(f"\nMissing values: {df.isnull().sum().sum()}")

# -----------------------------------------------------------------------------
# STEP 5: Save as Parquet in raw zone
# -----------------------------------------------------------------------------

output_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'data', 'raw', 'entsoe_prices_bulgaria_2024.parquet'
)

df.to_parquet(output_path, index=False)
print(f"\nData saved to: {output_path}")
print("Done!")
