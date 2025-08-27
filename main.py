import requests
import json
import os
import statistics
import pandas as pd
import io


# --- Useful API Links ---
BASE_URL = 'https://waterservices.usgs.gov/nwis/dv/'

"""
API_URL = Daily Values Service
"""

# --- DAILY VALUE Configuration --- #
DAILY_CACHE_FILENAME = 'data/usgs_data.json'
#DAILY_VALUE_URL = 'https://waterservices.usgs.gov/nwis/dv/?format=json&sites=06699005&startDT=1975-01-01&endDT=2000-12-31&siteStatus=all'
DAILY_VALUE_URL = 'https://waterservices.usgs.gov/nwis/dv/?format=json&sites=06611200&startDT=1975-01-01&endDT=2000-12-31&siteStatus=all'


# --- SITE INFO Configuration --- #
try:
    # Get the parent directory (the project root)
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    # Join the project root with the desired subdirectory and filename
    CACHE_FILENAME = os.path.join(PROJECT_ROOT, 'data', 'usgs_sitedata.rdb')
except NameError:
    # Fallback for interactive environments where __file__ is not defined
    CACHE_FILENAME = 'data/usgs_sitedata.rdb'

SITE_SERVICE_URL = "https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=08&siteStatus=all"

# --- Pandas Configuration ---
pd.set_option('display.max_columns', None)


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
        print(response.status_code)
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()

        data = response.json()
        print(data)

        # --- 3. Save the new data to the cache file ---
        print(f"Saving new data to cache file '{cache_file}'...")
        with open(cache_file, 'w') as f:
            json.dump(data, f, indent=4)

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None


def get_usgs_rdb_data(url, cache_file):
    """
    Fetches tab-delimited (RDB) data from a USGS URL, using a local cache.

    Args:
        url (str): The API endpoint URL that returns RDB data.
        cache_file (str): The filename for storing and retrieving the cached RDB data.

    Returns:
        pandas.DataFrame: A DataFrame containing the processed data, or None if an error occurs.
    """
    # --- 1. Check if a cached file already exists ---
    if os.path.exists(cache_file):
        print(f"Cache hit! Loading data from '{cache_file}'...")
        try:
            # If the file exists, we can read it directly
            df = pd.read_csv(cache_file, sep='\t', comment='#')
            # The cached file will still have the extra header, so we remove it
            df = df.iloc[1:].reset_index(drop=True)
            return df
        except Exception as e:
            print(f"Warning: Could not read cache file '{cache_file}'. Refetching. Error: {e}")

    # --- 2. If no cache, make the API call ---
    print(f"Cache miss. Fetching fresh data from API...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # --- 3. Ensure the cache directory exists before saving ---
        # Get the directory part of the cache_file path
        cache_dir = os.path.dirname(cache_file)
        # If the directory is not empty and doesn't exist, create it
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            print(f"Created cache directory: '{cache_dir}'")

        # --- 4. Save the raw response text to the cache file ---
        # We save the exact text, including comments, so it's a true representation of the API response.
        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"Saved new data to cache file '{cache_file}'.")

        # --- 5. Process the data we just fetched ---
        # Use io.StringIO to read the text content as if it were a file
        df = pd.read_csv(io.StringIO(response.text), sep='\t', comment='#')

        # Remove the second header row and reset the index
        df = df.iloc[1:].reset_index(drop=True)

        return df

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None

def extract_daily_data():
    """
    This Function Parses the JSON/PythonDictionary and places the data into workable variables that we can use
    to further analyze and manipulate the data
    :return:
    """
    usgs_data = get_usgs_data(DAILY_VALUE_URL, DAILY_CACHE_FILENAME)

    # --- 1. Grab the Timeseries Data
    time_series_data = usgs_data['value']['timeSeries'][0]

    # --- 2. Extract site information ---
    source_info = time_series_data['sourceInfo']
    site_name = source_info['siteName']
    site_code = source_info['siteCode'][0]['value']

    # --- 2.5 Extract Parameter Information ---
    units = time_series_data['variable']['unit']['unitCode']
    print(units)

    # --- 3. Extract the time series values ---
    # The values are in a list of dictionaries
    values = time_series_data['values'][0]['value']

    if not values:
        print("No time series values found in the file.")
        return None

    # --- 4. Create the DataFrame ---
    df = pd.DataFrame(values)

    # --- 5. Clean and format the DataFrame ---
    # Rename columns for clarity
    df.rename(columns={'value': 'streamflow_cfs', 'dateTime': 'timestamp'}, inplace=True)

    # Convert the 'timestamp' column to a proper datetime object
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Convert the 'streamflow_cfs' column to a numeric type
    df['streamflow_cfs'] = pd.to_numeric(df['streamflow_cfs'])

    # Add the site info to each row
    df['site_name'] = site_name
    df['site_code'] = site_code
    df['units'] = units


    # Reorder columns for a cleaner look
    df = df[['timestamp', 'streamflow_cfs','units', 'qualifiers', 'site_name', 'site_code']]

    return df

def extract_site_data():
    """
    This Function Parses the JSON/PythonDictionary and places the data into workable variables that we can use
    to further analyze and manipulate the data
    :return:
    """
    # Call the caching function to get the data
    site_data_df = get_usgs_rdb_data(SITE_SERVICE_URL, CACHE_FILENAME)

    if site_data_df is not None:
        print("\n--- Data successfully loaded into DataFrame ---")
        print(site_data_df['site_no'])


# --- Main script execution ---
if __name__ == "__main__":

    processed_data = extract_daily_data()
    print(processed_data.head())


