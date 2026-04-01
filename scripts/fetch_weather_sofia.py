

import requests        # lets Python send HTTP requests (talk to APIs)
import pandas as pd    # for creating and working with tables of data
import os              # for working with file paths and folders


# STEP 1: Define the API parameters

# The base URL for Open-Meteo's historical archive API
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# These are the parameters we send with our request
params = {
    "latitude": 42.7065,        # Sofia's latitude coordinate
    "longitude": 23.3219,       # Sofia's longitude coordinate
    "start_date": "2024-01-01", # Start of our data range
    "end_date": "2024-12-31",   # End of our data range (full year 2024)
    "hourly": "shortwave_radiation",  # The weather variable we want
    "timezone": "Europe/Sofia"  # Return timestamps in Sofia local time
}

# STEP 2: Send the request to the API

print("Sending request to Open-Meteo API...")

# HTTP GET request to the URL with our parameters
response = requests.get(BASE_URL, params=params)

response.raise_for_status()

print(f"Request successful! Status code: {response.status_code}")

# STEP 3: Parse the JSON response

data = response.json()
hourly_data = data["hourly"]

print(f"Data received. Number of hourly records: {len(hourly_data['time'])}")


# STEP 4: Convert to a pandas DataFrame

df = pd.DataFrame({
    "timestamp": hourly_data["time"],                      # datetime column
    "shortwave_radiation_wm2": hourly_data["shortwave_radiation"]  # solar radiation in W/M2^
})

# Convert the timestamp column from a plain string to an actual datetime type
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Add a column for the city name so we know where this data is from
df["city"] = "Sofia"

# Add coordinate columns for reference
df["latitude"] = 42.7065
df["longitude"] = 23.3219

print(f"DataFrame created. Shape: {df.shape}")
print(df.head())

# STEP 5: Save as Parquet


# Define where to save the file
output_path = os.path.join("data", "raw", "weather_sofia_2024.parquet")

df.to_parquet(output_path, index=False)

print(f"Data saved to: {output_path}")
print("Done!")