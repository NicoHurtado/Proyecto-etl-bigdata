import requests
import boto3
import json
import os

# Load S3 configuration
try:
    with open('../config/buckets.json', 'r') as f: # Adjusted path assuming script is in capture/
        config = json.load(f)
    RAW_BUCKET = config['raw_api_bucket']
except FileNotFoundError:
    print("Error: ../config/buckets.json not found. Please create it with your bucket names.")
    exit(1)
except KeyError as e:
    print(f"Error: Missing key {e} in ../config/buckets.json.")
    exit(1)

s3_client = boto3.client('s3')

# Parámetros API
latitude = 6.25
longitude = -75.56
start_date = "2022-01-01"
end_date = "2022-12-31"
daily_vars = "temperature_2m_max,precipitation_sum"
timezone = "America/Bogota"

url = (
    f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}"
    f"&longitude={longitude}&start_date={start_date}&end_date={end_date}"
    f"&daily={daily_vars}&timezone={timezone}"
)

# Descargar datos y subir a S3
filename_s3 = f"open_meteo_data_{start_date}_to_{end_date}.json" # Added a prefix for S3 organization

print(f"Fetching data from Open-Meteo API for {start_date} to {end_date}...")
response = requests.get(url)
response.raise_for_status() # Check for request errors

print(f"Uploading data to s3://{RAW_BUCKET}/{filename_s3}")

try:
    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=filename_s3,
        Body=response.text,
        ContentType='application/json'
    )
    print(f"Archivo subido a S3 exitosamente: s3://{RAW_BUCKET}/{filename_s3}")
except Exception as e:
    print(f"Error al subir archivo a S3: {e}")
    exit(1)
