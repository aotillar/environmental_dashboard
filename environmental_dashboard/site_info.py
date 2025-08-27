import pandas as pd
import requests
import os
import io  # Needed to treat the string response as a file

# --- Configuration ---
try:
    # Get the parent directory (the project root)
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    # Join the project root with the desired subdirectory and filename
    CACHE_FILENAME = os.path.join(PROJECT_ROOT, 'data', 'usgs_sitedata.rdb')
except NameError:
    # Fallback for interactive environments where __file__ is not defined
    CACHE_FILENAME = 'data/usgs_sitedata.rdb'

API_URL = "https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=08&siteStatus=all"


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


# --- Main script execution ---
if __name__ == "__main__":
    # Call the caching function to get the data
    site_data_df = get_usgs_rdb_data(API_URL, CACHE_FILENAME)

    if site_data_df is not None:
        print("\n--- Data successfully loaded into DataFrame ---")
        print("First 5 rows:")
        print(site_data_df.head())
        print("\nDataFrame Info:")
        site_data_df.info()
        print("---------------------------------------------")
    else:
        print("Failed to retrieve data. Please check the errors above.")
