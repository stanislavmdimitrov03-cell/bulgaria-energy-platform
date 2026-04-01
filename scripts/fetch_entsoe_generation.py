
import pandas as pd
import os
from entsoe import EntsoePandasClient
from dotenv import load_dotenv


# STEP 1: Load API key and create client


dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(dotenv_path)
api_key = os.getenv("ENTSOE_API_KEY")

if not api_key:
    raise ValueError("ENTSOE_API_KEY not found in .env file")

client = EntsoePandasClient(api_key=api_key)
print("ENTSO-E client created successfully")


# STEP 2: Define the time range


start = pd.Timestamp('20240101', tz='Europe/Sofia')
end   = pd.Timestamp('20241231', tz='Europe/Sofia')

print(f"Fetching generation by type for Bulgaria: {start.date()} to {end.date()}")


# STEP 3: Fetch generation data


generation = client.query_generation('BG', start=start, end=end, psr_type=None)

print(f"Raw data shape: {generation.shape}")
print(f"Generation types available: {list(generation.columns)}")


# STEP 4: Clean and reshape the DataFrame



if isinstance(generation.columns, pd.MultiIndex):
    generation.columns = ['_'.join(col).strip() for col in generation.columns]

generation = generation.reset_index()
generation = generation.rename(columns={'index': 'timestamp'})

# Remove timezone info from timestamp
generation['timestamp'] = pd.to_datetime(generation['timestamp']).dt.tz_localize(None)

# Add metadata
generation['country'] = 'Bulgaria'

print(f"\nCleaned DataFrame shape: {generation.shape}")
print(f"\nColumns: {list(generation.columns)}")
print(f"\nFirst 3 rows:")
print(generation.head(3))


# STEP 5: Basic exploration


# Show average generation by source (skip timestamp and country columns)
numeric_cols = generation.select_dtypes(include='number').columns
print(f"\nAverage generation by source (MW):")
print(generation[numeric_cols].mean().round(1).sort_values(ascending=False))

print(f"\nMissing values per column:")
print(generation.isnull().sum())


# STEP 6: Save as Parquet


output_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'data', 'raw', 'entsoe_generation_bulgaria_2024.parquet'
)

generation.to_parquet(output_path, index=False)
print(f"\nData saved to: {output_path}")
print("Done!")
