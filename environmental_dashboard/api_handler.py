import requests
import json
import os
import pandas as pd
import io
import hashlib

"""
This Document handles all of the API calls and caching of those files. It currently has 3 functions that are callable.
 
data_url_generator -> 
    This function generates the API url based on several input parameters. This is how all of the
different sites, and data types can be accessed. Currently the only thing that is needed externally for this function
is a site_id, which is the Unique USGS site number associated with the monitoring station/location. In the modern era
these sites are automated, and send data in real time to the internet. In the past, these could have been sites that
staff visited to manually gather data. There is a wealth of historical data available for many of these sites. 
    

get_usgs_json_data -> 
    This function takes a url as an argument and returns a json file. For the Daily Data, this is generally the format
that is provided. This function will first check to see if there is a file stored in the ./cache directory. If there is
a file associated with the url, then it will just load the file from the cache, saving time and API calls. If there
is no file in the cache, then this function will make the API call. This function returns a python dictionary that
was created from the JSON file, using the json library.
 
get_usgs_rdb_data ->
    This function takes a url as an argument and returns a pandas DF file. For the Daily Data, this is generally the 
format hat is provided. This function will first check to see if there is a file stored in the ./cache directory. 
If there is a file associated with the url, then it will just load the file from the cache, saving time and API
calls. If there is no file in the cache, then this function will make the API call. This function returns a pandas
dataframe that was created from the rdb file, using the pandas library. This filetype is preferred because
it can easily be graphed with many different libraries.
    
 
"""
# --- Pandas Configuration ---
# Just a Temporary Configuration to let Pandas print all the data
pd.set_option('display.max_columns', None)


def data_url_generator(sites, values='dv', file_format='json', period='P365D', site_status='all'):
    """
    A function to Generate a URL based on input parameters.
    Args:
        values: IV for instantaneous values, or DV for Daily Values.
        sites: a singular or multiple USGS Site IDS that is the target of the API call. This is the site of interest.
        file_format: Options are JSON, waterml, rdb, excel
        period: Time period of interest, required in ISO 8601 Duration Format. P: Period. Number ex 10, Duration eg Y
            for year, D for Day, W for Week, M for Month, etc
        site_status: Whether the Site is Active or Inactive.

    Returns: a URL in string format, that can be called by the requests.get() method.

    """
    url = 'https://waterservices.usgs.gov/nwis/'
    filters = f'{values}/?format={file_format}&sites={sites}&period={period}&siteStatus={site_status}'
    return url + filters


def get_usgs_json_data(url):
    """
    Fetches data from a given URL, using a local cache to avoid redundant API calls.

    Args:
        url (str): The API endpoint URL to fetch data from.

    Returns:
        dict: The JSON data as a Python dictionary, or None if an error occurs.
    """
    # --- Initial Step ---
    """
        This Creates a unique Cached Filename based on the API URL. That way if another API call is made with the same
        parameters, then it reads from the cache instead of making an API call. This makes it faster and saves the 
        USGS on API Requests.

    """
    # Get the parent directory (the project root)
    FILE_ROOT = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(FILE_ROOT)

    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()

    # 2. Construct the full cache file path
    temp_cache_filename = f"{url_hash}.json"
    cache_filename = os.path.join(PROJECT_ROOT, 'cache', temp_cache_filename)

    # --- 1. Check if a cached file already exists ---
    if os.path.exists(cache_filename):
        print(f"Cache hit! Loading data from '{cache_filename}'...")
        try:
            with open(cache_filename, 'r') as f:
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
        # --- 3. Save the new data to the cache file ---
        print(f"Saving new data to cache file '{cache_filename}'...")
        print("Here is your issue", cache_filename)
        with open(cache_filename, 'w') as f:
            json.dump(data, f, indent=4)

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None


def get_usgs_rdb_data(url):
    """
    Fetches tab-delimited (RDB) data from a USGS URL, using a local cache.

    Args:
        url (str): The API endpoint URL that returns RDB data.

    Returns:
        pandas.DataFrame: A DataFrame containing the processed data, or None if an error occurs.
    """
    # --- Initial Step ---
    """
    This Creates a unique Cached Filename based on the API URL. That way if another API call is made with the same
    parameters, then it reads from the cache instead of making an API call. This makes it faster and saves the 
    USGS on API Requests.
    
    """

    # Get the parent directory (the project root)
    FILE_ROOT = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(FILE_ROOT)

    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()

    # 2. Construct the full cache file path
    # This places the Cache file in a predesignated data folder of the project directory.
    temp_cache_filename = f"{url_hash}.rdb"
    cache_filename = os.path.join(PROJECT_ROOT, 'cache', temp_cache_filename)

    # --- 1. Check if a cached file already exists ---
    if os.path.exists(cache_filename):
        print(f"Cache hit! Loading data from '{cache_filename}'...")
        # This differs from the JSON Code becasue rdb is tab delimited byte stream data
        try:
            # If the file exists, we can read it directly
            df = pd.read_csv(cache_filename, sep='\t', comment='#')
            # The cached file will still have the extra header, so we remove it
            df = df.iloc[1:].reset_index(drop=True)
            return df
        except Exception as e:
            print(f"Warning: Could not read cache file '{cache_filename}'. Refetching. Error: {e}")

    # --- 2. If no cache, make the API call ---
    print(f"Cache miss. Fetching fresh data from API...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # --- 3. Ensure the cache directory exists before saving ---
        # Get the directory part of the cache_file path
        cache_dir = os.path.dirname(cache_filename)
        # If the directory is not empty and doesn't exist, create it
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            print(f"Created cache directory: '{cache_dir}'")

        # --- 4. Save the raw response text to the cache file ---
        # We save the exact text, including comments, so it's a true representation of the API response.
        with open(cache_filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"Saved new data to cache file '{cache_filename}'.")

        # --- 5. Process the data we just fetched ---
        # Use io.StringIO to read the text content as if it were a file
        df = pd.read_csv(io.StringIO(response.text), sep='\t', comment='#')

        # Remove the second header row and reset the index
        df = df.iloc[1:].reset_index(drop=True)

        return df

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None

# Just test code to make sure this module works properly


if __name__ == "__main__":
    json_url = data_url_generator(values='iv', sites='09163500')
    rdb_url = data_url_generator(file_format='rdb', values='iv', sites='09163500')
    get_usgs_json_data(json_url)
    get_usgs_rdb_data(rdb_url)
