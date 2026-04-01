

import duckdb
import os
import boto3
import pandas as pd

# Connect to DuckDB
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'bulgaria_energy.db'))
con = duckdb.connect(db_path)
con.execute("SET temp_directory='C:/Users/stann/Projects/bulgaria-energy-platform/tmp'")

# S3 client
s3 = boto3.client('s3')
BUCKET = 'bulgaria-energy-platform-sd-2024'


curated_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'curated'))
os.makedirs(curated_dir, exist_ok=True)

# Tables to export
tables = [
    'fct_generation_hourly',
    'fct_prices_hourly',
    'fct_weather_hourly',
    'dim_date',
    'dim_city'
]

for table in tables:
    print(f"Exporting {table}...")

    df = con.execute(f"SELECT * FROM {table}").df()
#Parquet
    local_path = os.path.join(curated_dir, f"{table}.parquet")
    df.to_parquet(local_path, index=False)

    # Upload to S3
    s3_key = f"curated/{table}/data.parquet"
    s3.upload_file(local_path, BUCKET, s3_key)

    print(f"  Uploaded {len(df)} rows to s3://{BUCKET}/{s3_key}")

con.close()
print("\nAll tables exported to S3!")

