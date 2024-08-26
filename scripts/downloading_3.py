import requests
import pandas as pd
import io

# Base URL of the API endpoint
base_url = "https://data.cityofnewyork.us/resource/bkfu-528j.csv"

# Parameters for the API request
params = {
    "$limit": 50000,  # Maximum rows per request
    "$offset": 0      # Start offset
}

# Initialize an empty list to store the data
all_data = []

while True:
    # Make the API request
    response = requests.get(base_url, params=params)
    
    # Break the loop if the response is empty or invalid
    if response.status_code != 200 or not response.content:
        break
    
    # Read the CSV content directly into a Pandas DataFrame
    data = pd.read_csv(io.StringIO(response.text))
    if data.empty:
        break
    # Append the retrieved data to the list
    all_data.append(data)
    
    # Update the offset for the next request
    params["$offset"] += params["$limit"]
    print(f"Retrieved {len(data)} rows. Total rows: {params['$offset']}")

# Concatenate all the DataFrames in the list
df = pd.concat(all_data, ignore_index=True)

# Save the DataFrame to the specified path
output_path = "../data/landing/external/event_data.parquet"
df.to_csv(output_path, index=False)

# Display a message confirming the save
print(f"Data successfully saved to {output_path}")
