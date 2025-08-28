import pandas as pd
import matplotlib.pyplot as plt
from environmental_dashboard import api_handler

"""
This script handles the data manipulation tasks from the API calls. It takes the raw responses from the USGS GET
request and extracts the data that we are looking for.\

It seems that the best way to handle all of these different sites and parameters is to make a function that always
makes a pandas dataframe, then it loops through the columns and looks for common things, like datetime, and converts
those to things that are useful for plotting (formatting etc). 

    Then when it comes to the actual display of the data, then potentially the graph that displays can 
    dynamically see which parameters are present and then populate a dropdown with those decoded options.
    
    So for instance in the current site USGS-09163500, here are all of the parameters
    
    211943      00010     Temperature, water, degrees Celsius
    211940      00060     Discharge, cubic feet per second
    279778      00065     Gage height, feet
    211944      00095     Specific conductance, water, unfiltered, microsiemens per centimeter at 25 degrees Celsius
    291097      00300     Dissolved oxygen, water, unfiltered, milligrams per liter
    291125      00400     pH, water, unfiltered, field, standard units
    291113      63680     Turbidity, water, unfiltered, monochrome near infra-red LED light, 780-900 nm,
        detection angle 90 +-2.5 degrees, formazin nephelometric units (FNU)
        
    Not Every site has these parameters. It doesnt make sense to make a function, or multiple if then nests to handle 
    all of these conditions. Instead it makes sense to keep these parameters in the dataframe, and then only extract,
    or graph ones that meet a USER choosen criteria. This will save on complexity etc. 
    
    In the Dash app, in the example above, each of these parameters will be displayed in a dropdown, and the user 
    can choose which one to graph.

"""
# TODO: This will be the biggest portion of the program before the DASH application. Add multiple parameters
# TODO: Add Instantaneous value processing
# TODO: Add the ability for more flexibility in the processing and less reliability on Hard Coding the Parsing
# TODO: Several Sites will have different Parameters, have a procesing function that can handle all of them.
# --- Pandas Configuration ---
pd.set_option('display.max_columns', None)


def extract_json_data():
    """
    This Function Parses the JSON/PythonDictionary and places the data into workable variables that we can use
    to further analyze and manipulate the data
    :return:
    """
    url = api_handler.data_url_generator(values='iv', sites='09163500')
    usgs_data = api_handler.get_usgs_json_data(url)

    # --- 1. Grab the Timeseries Data
    time_series_data = usgs_data['value']['timeSeries'][0]

    # --- 2. Extract site information ---
    source_info = time_series_data['sourceInfo']
    site_name = source_info['siteName']
    site_code = source_info['siteCode'][0]['value']

    # --- 2.5 Extract Parameter Information ---
    units = time_series_data['variable']['unit']['unitCode']

    # --- 3. Extract the time series values ---
    # The values are in a list of dictionaries
    values = time_series_data['values'][0]['value']

    if not values:
        print("No time series values found in the file.")
        return None

    # --- 4. Create the DataFrame ---
    df = pd.DataFrame(values)

    # --- 5. Clean and format the DataFrame ---
    # --- Rename columns for clarity
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
    df = df[['timestamp', 'streamflow_cfs', 'units', 'qualifiers', 'site_name', 'site_code']]

    return df


def extract_rdb_data():
    """

    :return:
    """
    # Call the caching function to get the data
    rdb_url = api_handler.data_url_generator(file_format='rdb', values='iv', sites='09163500')
    site_data_df = api_handler.get_usgs_rdb_data(rdb_url)
    site_data_df['datetime'] = pd.to_datetime(site_data_df['datetime'])
    site_data_df['211943_00010'] = site_data_df['211943_00010'].astype(float)
    print(site_data_df.head())
    print(site_data_df.columns)
    print('--------------------------------')
    subset = site_data_df[['datetime','211943_00010']]
    print(subset.head())
    print(subset['211943_00010'].dtype)
    subset.plot(x='datetime',y='211943_00010',kind='line',title='Subset of Columns')
    plt.show()
    return site_data_df


# --- Data Processor script execution ---
if __name__ == "__main__":
    rdb_data = extract_rdb_data()

    # processed_data = extract_json_data()
    # processed_data.plot(x='timestamp',
    #                     y='streamflow_cfs',
    #                     kind='line')
    # plt.show()
