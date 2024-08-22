
"""
============================================================================
Download data from the cloudfront server and save it to the landing folder 
============================================================================
"""

# import libraries
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sbs
import geopandas as gpd
import folium
import os
import requests

# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("ADS Project1")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

spark.conf.set("spark.sql.parquet.compression.codec","gzip")

# Create the data folders
base_dir = '../data'
data_folders = [
'landing/tlc_data/2023',
'landing/tlc_data/2024',
'raw/tlc_data',
'curated/tlc_data/first_clean',
'curated/tlc_data/final_data'
]

for folder in data_folders:
    path = os.path.join(base_dir, folder)
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'Created folder: {path}')
        
# ==============================================================================
# TLC Datasource Download
# ==============================================================================

# Download the 2023 yellow trip data from the cloudfront server
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
YEAR = ['2023']
MONTH = range(5, 13)
output_relative_dir = '../data/landing/tlc_data'

for year in YEAR:
    for month in MONTH:
        print(f'Starting download for {year}-{str(month).zfill(2)}')
        month_str = str(month).zfill(2)
        url = f'{URL_TEMPLATE}{year}-{month_str}.parquet'
        output_dir = f"{output_relative_dir}/{year}/{year}-{month_str}.parquet"
        
        response = requests.get(url, verify=True)
        with open(output_dir, 'wb') as file:
            file.write(response.content)
            
        print(f'Finished download for {year}-{month_str}')


# Download the 2024 yellow trip data from the cloudfront server
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
YEAR = ['2024']
MONTH = range(1, 6)
output_relative_dir = '../data/landing/tlc_data'

for year in YEAR:
    for month in MONTH:
        print(f'Starting download for {year}-{str(month).zfill(2)}')
        month_str = str(month).zfill(2)
        url = f'{URL_TEMPLATE}{year}-{month_str}.parquet'
        output_dir = f"{output_relative_dir}/{year}/{year}-{month_str}.parquet"
        
        response = requests.get(url, verify=True)
        with open(output_dir, 'wb') as file:
            file.write(response.content)
            
        print(f'Finished download for {year}-{month_str}')


# Download the 2023 green trip data from the cloudfront server
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"
YEAR = ['2023']
MONTH = range(5, 13)
output_relative_dir = '../data/landing/tlc_data'

for year in YEAR:
    for month in MONTH:
        print(f'Starting download for {year}-{str(month).zfill(2)}')
        month_str = str(month).zfill(2)
        url = f'{URL_TEMPLATE}{year}-{month_str}.parquet'
        output_dir = f"{output_relative_dir}/{year}/G-{year}-{month_str}.parquet"
        
        response = requests.get(url, verify=True)
        with open(output_dir, 'wb') as file:
            file.write(response.content)
            
        print(f'Finished download for {year}-{month_str}')

# Download the 2024 green trip data from the cloudfront server
URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"
YEAR = ['2024']
MONTH = range(1, 6)
output_relative_dir = '../data/landing/tlc_data'

for year in YEAR:
    for month in MONTH:
        print(f'Starting download for {year}-{str(month).zfill(2)}')
        month_str = str(month).zfill(2)
        url = f'{URL_TEMPLATE}{year}-{month_str}.parquet'
        output_dir = f"{output_relative_dir}/{year}/G-{year}-{month_str}.parquet"
        
        response = requests.get(url, verify=True)
        with open(output_dir, 'wb') as file:
            file.write(response.content)
            
        print(f'Finished download for {year}-{month_str}')

# ==============================================================================
# Download weather data from Central Park 
# ==============================================================================

import requests
# URL of the CSV file
url = 'https://www.ncei.noaa.gov/access/past-weather/USW00094728/data.csv'

# Directory where the file will be saved
output_relative_dir = '../data/landing/external'
output_file_path = os.path.join(output_relative_dir, 'NYC weather.csv')

# Create the directory if it does not exist
os.makedirs(output_relative_dir, exist_ok=True)

# Send a GET request to fetch the file
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Save the content to a file
    with open(output_file_path, 'wb') as file:
        file.write(response.content)
    
    # Read the saved CSV file into a pandas DataFrame
    df = pd.read_csv(output_file_path)
    # Display the first few rows of the DataFrame
    print(df.head())
else:
    print(f"Failed to download file. Status code: {response.status_code}")

# ==============================================================================
# Download traffic data from NYC Open Data
# ==============================================================================

# URL of the CSV file with limit parameter
url = 'https://data.cityofnewyork.us/resource/btm5-ppia.csv?$limit=45000'

# Directory where the file will be saved
output_relative_dir = '../data/landing/external'
output_file_path = os.path.join(output_relative_dir, 'NYC Traffic.csv')

# Create the directory if it does not exist
os.makedirs(output_relative_dir, exist_ok=True)

# Send a GET request to fetch the file
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Save the content to a file
    with open(output_file_path, 'wb') as file:
        file.write(response.content)
    
    # Read the saved CSV file into a pandas DataFrame
    df = pd.read_csv(output_file_path)
    # Display the first few rows of the DataFrame
    print(df.head())
else:
    print(f"Failed to download file. Status code: {response.status_code}")



