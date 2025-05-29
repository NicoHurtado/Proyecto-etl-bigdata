import pandas as pd
from sqlalchemy import create_engine
import boto3
import json
from io import StringIO # Required to treat string as a file

# Load S3 configuration
try:
    with open('../config/buckets.json', 'r') as f: # Adjusted path
        config = json.load(f)
    # The full S3 path is in the config, we need to parse bucket and key
    raw_db_s3_path = config['raw_db_bucket']
    if not raw_db_s3_path.startswith("s3://"):
        print("Error: raw_db_bucket in config must be a full S3 path (e.g., s3://bucket-name/path/to/dir/)")
        exit(1)
    
    path_parts = raw_db_s3_path.replace("s3://", "").split("/", 1)
    RAW_DB_BUCKET_NAME = path_parts[0]
    # Ensure the key ends with a '/' if it's meant to be a prefix/directory
    RAW_DB_S3_KEY_PREFIX = path_parts[1] if len(path_parts) > 1 and path_parts[1] else ''
    if RAW_DB_S3_KEY_PREFIX and not RAW_DB_S3_KEY_PREFIX.endswith('/'):
        RAW_DB_S3_KEY_PREFIX += '/'

except FileNotFoundError:
    print("Error: ../config/buckets.json not found. Please create it with your bucket names.")
    exit(1)
except KeyError as e:
    print(f"Error: Missing key {e} in ../config/buckets.json.")
    exit(1)
except IndexError:
    print(f"Error: Could not parse S3 bucket and key from raw_db_bucket: {raw_db_s3_path}")
    exit(1)

s3_client = boto3.client('s3')

print(f"Connecting to database...")
engine = create_engine(
    "mysql+pymysql://admin:Proyecto3nj@weather-db.crjy13xo6n8j.us-east-1.rds.amazonaws.com:3306/weatherdb"
)

print("Fetching data from database...")
df = pd.read_sql("SELECT * FROM weather_data", engine)

if df.empty:
    print("No data fetched from database. Nothing to upload.")
else:
    # Define S3 object key
    s3_file_name = "weather_data_from_db.csv"
    s3_object_key = f"{RAW_DB_S3_KEY_PREFIX}{s3_file_name}"

    print(f"Converting DataFrame to CSV and uploading to s3://{RAW_DB_BUCKET_NAME}/{s3_object_key}")
    
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()

    try:
        s3_client.put_object(
            Bucket=RAW_DB_BUCKET_NAME,
            Key=s3_object_key,
            Body=csv_string,
            ContentType='text/csv'
        )
        print(f"✅ Archivo '{s3_file_name}' subido a S3 exitosamente: s3://{RAW_DB_BUCKET_NAME}/{s3_object_key}")
    except Exception as e:
        print(f"Error al subir archivo CSV a S3: {e}")
        exit(1)
