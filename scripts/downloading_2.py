# Import necessary libraries
from pyspark.sql import SparkSession
import os
import requests
import pandas as pd
import zipfile

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

# Download weather data from Central Park NYC
weather_url_2023 = 'https://www.ncei.noaa.gov/data/global-hourly/access/2023/72505394728.csv'
weather_url_2024 = 'https://www.ncei.noaa.gov/data/global-hourly/access/2024/72505394728.csv'

weather_output_path_2023 = './data/landing/external/NYC_weather_2023.csv'
weather_output_path_2024 = './data/landing/external/NYC_weather_2024.csv'

download_external_data(weather_url_2023, weather_output_path_2023)
download_external_data(weather_url_2024, weather_output_path_2024)

# Donwload taxizone files from NYC Open Data
# URLs and paths
taxi_zone_url = 'https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD'
taxi_zone_output_path = './data/landing/external/taxi_zones.csv'
lookup_url = 'https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=Shapefile'
lookup_output_path = './data/landing/external/taxi_zone_lookup.zip'
extract_to = './data/landing/external/'

# Download the taxi zone CSV file
download_external_data(taxi_zone_url, taxi_zone_output_path)

# Download the shapefile ZIP
download_external_data(lookup_url, lookup_output_path)

# Ensure the directory exists
os.makedirs(extract_to, exist_ok=True)

# Unzip the shapefile
with zipfile.ZipFile(lookup_output_path, 'r') as zip_ref:
    zip_ref.extractall(extract_to)

print("Unzipping completed.")
