import requests
import boto3
from datetime import date

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

# Fetch y guarda local
response = requests.get(url)
filename = f"open_meteo_{start_date}_to_{end_date}.json"
with open(filename, "w") as f:
    f.write(response.text)

# Subir a S3
s3 = boto3.client('s3')
s3.upload_file(filename, 'weather-etl-raw-nicojaco', f"open_meteo/{filename}")
