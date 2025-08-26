import requests
import json
import os
import statistics

# --- Useful API Links ---
BASE_URL = 'https://api.waterdata.usgs.gov/ogcapi/v0'
OLD_BASE_URL = 'https://waterservices.usgs.gov/nwis/dv/'

"""
BASE_URL is the new USGS API. It has not been full updated with all of the historical data yet, so in the case of
stations that are not currently active, then usage of the OLD_BASE_URL is generally advisable.

The USGS doesnt have clear guidance on this, and it can be a big mess sorting through all of the data, and different API
calls.
"""

response = requests.get('https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items&hydrologic_unit_code=101900')
response2 = requests.get('https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items&monitoring_location_id=06693980')
response3 = requests.get('https://waterservices.usgs.gov/nwis/dv/?format=json&sites=06699005&startDT=1975-01-01&endDT=2000-12-31&siteStatus=all')


# --- Configuration ---
CACHE_FILENAME = 'usgs_data.json'
API_URL = 'https://waterservices.usgs.gov/nwis/dv/?format=json&sites=06699005&startDT=1975-01-01&endDT=2000-12-31&siteStatus=all'


def get_usgs_data(url, cache_file):
    """
    Fetches data from a given URL, using a local cache to avoid redundant API calls.

    Args:
        url (str): The API endpoint URL to fetch data from.
        cache_file (str): The filename for storing and retrieving cached data.

    Returns:
        dict: The JSON data as a Python dictionary, or None if an error occurs.
    """
    # --- 1. Check if a cached file already exists ---
    if os.path.exists(cache_file):
        print(f"Cache hit! Loading data from '{cache_file}'...")
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            return data
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not read cache file. Refetching data. Error: {e}")

    # --- 2. If no cache, make the API call ---
    print(f"Cache miss. Fetching fresh data from API...")
    try:
        response = requests.get(url)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        data = response.json()

        # --- 3. Save the new data to the cache file ---
        print(f"Saving new data to cache file '{cache_file}'...")
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=4)

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None


def extract_data():
    """
    This Function Parses the JSON/PythonDictionary and places the data into workable variables that we can use
    to further analyze and manipulate the data
    :return:
    """
    usgs_data = get_usgs_data(API_URL, CACHE_FILENAME)

    if usgs_data:
        print("\n--- Data successfully loaded ---")
        data = usgs_data['value']['timeSeries'][0]['values'][0]['value']

        cfs_data = []
        for item in data:
            for key, value in item.items():
                if key == 'value':
                    cfs_data.append(float(value))

        print(cfs_data)
        print(statistics.mean(cfs_data))


# --- Main script execution ---
if __name__ == "__main__":
    extract_data()

