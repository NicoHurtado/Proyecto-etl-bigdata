from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, col

# Crear sesión de Spark con soporte para S3
spark = SparkSession.builder.appName("WeatherETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Leer archivo JSON desde S3 (zona raw)
weather_json = "s3a://weather-etl-raw-nicojaco/open_meteo/open_meteo_2022-01-01_to_2022-12-31.json"
weather_raw = spark.read.option("multiline", "true").json(weather_json)

# Transformar datos del JSON
weather_df = weather_raw.select(
    explode(
        arrays_zip(
            col("daily.time"),
            col("daily.temperature_2m_max"),
            col("daily.precipitation_sum")
        )
    ).alias("daily_record")
).select(
    col("daily_record.time").alias("date"),
    col("daily_record.temperature_2m_max").alias("temperature_2m_max"),
    col("daily_record.precipitation_sum").alias("precipitation_sum")
)

# Leer CSV desde S3 (zona raw)
stations_csv = "s3a://weather-etl-raw-nicojaco/stations/weather_data.csv"
stations_df = spark.read.csv(
    stations_csv,
    header=True,
    inferSchema=True
)

# Renombrar columnas para evitar conflictos
for col_name in ['date', 'temperature_2m_max', 'precipitation_sum']:
    if col_name in stations_df.columns:
        stations_df = stations_df.withColumnRenamed(col_name, f'station_{col_name}')

# Unión cruzada
combined_df = weather_df.crossJoin(stations_df)

# Guardar resultado en S3 (zona trusted)
combined_df.write.mode("overwrite").parquet("s3a://weather-etl-trusted-nicojaco/processed_weather/")

print("✅ ETL completado correctamente.")
