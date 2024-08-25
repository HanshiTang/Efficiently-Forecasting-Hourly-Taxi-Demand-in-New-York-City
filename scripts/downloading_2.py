# Import necessary libraries
from pyspark.sql import SparkSession
import os
import requests
import pandas as pd

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
