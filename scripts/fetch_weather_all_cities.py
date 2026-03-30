# =============================================================================
# fetch_weather_all_cities.py
# Pulls hourly weather data for 5 Bulgarian cities from Open-Meteo archive API
# Uses a reusable function to avoid repeating code for each city
# =============================================================================

import requests
import pandas as pd
import os
import time  # we use this to pause between API calls so we don't overload the server

# -----------------------------------------------------------------------------
# STEP 1: Define our cities
# Each city is a dictionary with its name and coordinates
# -----------------------------------------------------------------------------

CITIES = [
    {"city": "Sofia",   "latitude": 42.7065, "longitude": 23.3219},
    {"city": "Plovdiv", "latitude": 42.1354, "longitude": 24.7453},
    {"city": "Varna",   "latitude": 43.2141, "longitude": 27.9147},
    {"city": "Burgas",  "latitude": 42.5048, "longitude": 27.4626},
    {"city": "Pleven",  "latitude": 43.4170, "longitude": 24.6167},
]

# The base URL for Open-Meteo's historical archive API
BASE_URL = "httpnotepad scripts\setup_duckdb.pys://archive-api.open-meteo.com/v1/archive"

# -----------------------------------------------------------------------------
# STEP 2: Define a reusable function
# This function takes city info as input and returns a clean DataFrame
# We can call this function once per city instead of repeating all the code
# -----------------------------------------------------------------------------

def fetch_weather_for_city(city, latitude, longitude):
    """
    Fetches hourly weather data for a single city for all of 2024.
    Returns a pandas DataFrame with the results.
    """

    print(f"Fetching data for {city}...")

    # Define the parameters for this specific city
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        # Request three weather variables at once (comma separated)
        "hourly": "shortwave_radiation,windspeed_10m,temperature_2m",
        "timezone": "Europe/Sofia"
    }

    # Send the HTTP request to the API
    response = requests.get(BASE_URL, params=params)

    # Raise an error if the request failed
    response.raise_for_status()

    # Parse the JSON response into a Python dictionary
    data = response.json()

    # Extract the hourly data section
    hourly = data["hourly"]

    # Build a DataFrame from the parallel lists in the response
    df = pd.DataFrame({
        "timestamp":                hourly["time"],
        "shortwave_radiation_wm2":  hourly["shortwave_radiation"],
        "windspeed_10m_kmh":        hourly["windspeed_10m"],
        "temperature_2m_c":         hourly["temperature_2m"],
    })

    # Convert timestamp from string to proper datetime type
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Add city metadata columns so we know where each row came from
    df["city"]      = city
    df["latitude"]  = latitude
    df["longitude"] = longitude

    print(f"  ✓ {len(df)} records fetched for {city}")

    return df  # send the DataFrame back to whoever called this function

# -----------------------------------------------------------------------------
# STEP 3: Call the function for each city and combine results
# -----------------------------------------------------------------------------

# This list will collect one DataFrame per city
all_dataframes = []

for city_info in CITIES:
    # Call our function with this city's details
    df = fetch_weather_for_city(
        city=city_info["city"],
        latitude=city_info["latitude"],
        longitude=city_info["longitude"]
    )

    # Add this city's DataFrame to our collection
    all_dataframes.append(df)

    # Wait 1 second between requests — polite API usage
    time.sleep(1)

# Combine all 5 DataFrames into one big DataFrame
# ignore_index=True resets the row numbers so they run 0 to 43919
combined_df = pd.concat(all_dataframes, ignore_index=True)

print(f"\nAll cities fetched!")
print(f"Combined shape: {combined_df.shape}")
print(f"Cities in dataset: {combined_df['city'].unique()}")

# -----------------------------------------------------------------------------
# STEP 4: Save as Parquet in the raw zone
# -----------------------------------------------------------------------------

output_path = os.path.join("data", "raw", "weather_all_cities_2024.parquet")
combined_df.to_parquet(output_path, index=False)

print(f"\nData saved to: {output_path}")
print("Done!")