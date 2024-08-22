
"""
============================================================================
Download data from the cloudfront server and save it to the landing folder 
============================================================================
"""

# import libraries
from pyspark.sql import SparkSession
import requests
import os

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




