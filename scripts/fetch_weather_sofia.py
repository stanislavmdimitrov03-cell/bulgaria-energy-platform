# =============================================================================
# fetch_weather_sofia.py
# Pulls hourly solar radiation data for Sofia from Open-Meteo's archive API
# and saves it as a Parquet file in the raw data zone.
# =============================================================================

import requests        # lets Python send HTTP requests (talk to APIs)
import pandas as pd    # for creating and working with tables of data
import os              # for working with file paths and folders

# -----------------------------------------------------------------------------
# STEP 1: Define the API parameters
# -----------------------------------------------------------------------------

# The base URL for Open-Meteo's historical archive API
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# These are the parameters we send with our request
# Think of these as the "form fields" we fill in to specify what we want
params = {
    "latitude": 42.7065,        # Sofia's latitude coordinate
    "longitude": 23.3219,       # Sofia's longitude coordinate
    "start_date": "2024-01-01", # Start of our data range
    "end_date": "2024-12-31",   # End of our data range (full year 2024)
    "hourly": "shortwave_radiation",  # The weather variable we want
    "timezone": "Europe/Sofia"  # Return timestamps in Sofia local time
}

# -----------------------------------------------------------------------------
# STEP 2: Send the request to the API
# -----------------------------------------------------------------------------

print("Sending request to Open-Meteo API...")

# requests.get() sends an HTTP GET request to the URL with our parameters
# This is exactly what your browser did, but in Python code
response = requests.get(BASE_URL, params=params)

# Check if the request succeeded (HTTP status 200 means success)
# If something went wrong, this will raise an error with a clear message
response.raise_for_status()

print(f"Request successful! Status code: {response.status_code}")

# -----------------------------------------------------------------------------
# STEP 3: Parse the JSON response
# -----------------------------------------------------------------------------

# .json() converts the raw response text into a Python dictionary
# Just like the JSON we saw in the browser, now accessible in Python
data = response.json()

# Navigate into the nested structure to get the hourly data
# data["hourly"] gives us the dictionary with "time" and "shortwave_radiation"
hourly_data = data["hourly"]

print(f"Data received. Number of hourly records: {len(hourly_data['time'])}")

# -----------------------------------------------------------------------------
# STEP 4: Convert to a pandas DataFrame
# -----------------------------------------------------------------------------

# pd.DataFrame() creates a table from a dictionary
# Each key becomes a column, each list becomes the column's values
df = pd.DataFrame({
    "timestamp": hourly_data["time"],                      # datetime column
    "shortwave_radiation_wm2": hourly_data["shortwave_radiation"]  # solar radiation in W/m²
})

# Convert the timestamp column from a plain string to an actual datetime type
# This lets us do date operations later (extract hour, month, etc.)
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Add a column for the city name so we know where this data is from
# This will be useful when we combine data from multiple cities later
df["city"] = "Sofia"

# Add coordinate columns for reference
df["latitude"] = 42.7065
df["longitude"] = 23.3219

print(f"DataFrame created. Shape: {df.shape}")  # shape = (rows, columns)
print(df.head())                                 # show first 5 rows as a preview

# -----------------------------------------------------------------------------
# STEP 5: Save as Parquet
# -----------------------------------------------------------------------------

# Define where to save the file
# os.path.join builds a file path correctly regardless of operating system
output_path = os.path.join("data", "raw", "weather_sofia_2024.parquet")

# Save the DataFrame as a Parquet file
# Parquet is a compressed, efficient file format designed for data engineering
# We will explain it in detail in the next step
df.to_parquet(output_path, index=False)  # index=False means don't save the row numbers

print(f"Data saved to: {output_path}")
print("Done!")