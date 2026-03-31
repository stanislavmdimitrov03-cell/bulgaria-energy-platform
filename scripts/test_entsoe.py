from entsoe import EntsoePandasClient
import pandas as pd
import os
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
api_key = os.getenv("ENTSOE_API_KEY")

# Create client
client = EntsoePandasClient(api_key=api_key)

# Test: pull one day of Bulgarian day-ahead prices
start = pd.Timestamp('20240101', tz='Europe/Sofia')
end   = pd.Timestamp('20240102', tz='Europe/Sofia')

print("Fetching Bulgarian day-ahead prices for Jan 1 2024...")
prices = client.query_day_ahead_prices('BG', start=start, end=end)
print(prices)
print("Success!")
