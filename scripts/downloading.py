# Import necessary libraries
from pyspark.sql import SparkSession
import os
import requests
import pandas as pd

# Create a Spark session
spark = (
    SparkSession.builder.appName("ADS Project1")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

# Create the data folders
base_dir = './data'
data_folders = [
    'landing/tlc_data',
    'raw/tlc_data',
    'curated/tlc_data/first_clean',
    'curated/tlc_data/final_data',
    'landing/external'
]

for folder in data_folders:
    path = os.path.join(base_dir, folder)
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'Created folder: {path}')

# Function to download TLC trip data
def download_tlc_data(year, start_month, end_month, color='yellow'):
    URL_TEMPLATE = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_"
    output_relative_dir = f'./data/landing/tlc_data'
    
    for month in range(start_month, end_month + 1):
        month_str = str(month).zfill(2)
        url = f'{URL_TEMPLATE}{year}-{month_str}.parquet'
        output_path = f"{output_relative_dir}/{color[0].upper()}-{year}-{month_str}.parquet"
        
        print(f'Starting download for {year}-{month_str}')
        response = requests.get(url, verify=True)
        
        if response.status_code == 200:
            with open(output_path, 'wb') as file:
                file.write(response.content)
            print(f'Finished download for {year}-{month_str}')
        else:
            print(f'Failed to download {year}-{month_str}. Status code: {response.status_code}')

# Download the TLC trip data for 2023 and 2024
download_tlc_data(2023, 6, 12, color='yellow')
download_tlc_data(2024, 1, 5, color='yellow')
download_tlc_data(2023, 6, 12, color='green')
download_tlc_data(2024, 1, 5, color='green')

# Function to download external data
def download_external_data(url, output_file_path):
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(output_file_path, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded and saved file to {output_file_path}")
        
        # Read and display the data if it is a CSV
        if output_file_path.endswith('.csv'):
            df = pd.read_csv(output_file_path)
            print(df.head())
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

# Download weather data from Central Park
weather_url = 'https://www.ncei.noaa.gov/access/past-weather/USW00094728/data.csv'
weather_output_path = './data/landing/external/NYC_weather.csv'
download_external_data(weather_url, weather_output_path)

# Download traffic data from NYC Open Data
traffic_url = 'https://data.cityofnewyork.us/resource/btm5-ppia.csv?$limit=45000'
traffic_output_path = './data/landing/external/NYC_Traffic.csv'
download_external_data(traffic_url, traffic_output_path)
