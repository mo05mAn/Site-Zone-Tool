#!C:\Users\xun.lu\Anaconda3\envs\Site Zone Tool

import sys
import pandas as pd
import requests

# input Paths
PATH_TO_INPUT = './input.csv'
PATH_TO_OUTPUT = './output.csv'

# Data Paths
PATH_TO_SDDA = './data/small_dda_2023.csv'
PATH_TO_NMDDA = './data/non-metro_dda_2023.csv'
PATH_TO_QCT = './data/qct_2023.csv'
PATH_TO_QOZ = './data/qoz_2018.csv'

# FCC HTML API URL
FCC_API_PATH = 'https://geo.fcc.gov/api/census/block/find'

# Column INDICES
ZIP_COL_INDEX = 'zip_code'
FIPS_COL_INDEX = 'fips'
BLOCK_FIPS_COL_INDEX = 'fips'
CENSUS_TRACT_COL_INDEX = 'census_tract_number'
LATITUDE_COL_INDEX = 'latitude'
LONGITUDE_COL_INDEX = 'longitude'
QCT_COL_INDEX = 'qct_2023'
DDA_COL_INDEX = 'dda_2023'
QOZ_COL_INDEX = 'qoz_2023'


def main(argv):
    input_df = ''
    use_args = len(argv) == 3

    # Load Input from file if no arguments given
    if use_args:
         input_df = {
            ZIP_COL_INDEX: [argv[0]],
            LATITUDE_COL_INDEX: [argv[1]],
            LONGITUDE_COL_INDEX: [argv[2]]
         }
         input_df = pd.DataFrame(input_df)
    else:
         input_df = pd.read_csv(PATH_TO_INPUT, sep=',', low_memory=False)

    # Load lists
    sdda_list = pd.read_csv(PATH_TO_SDDA, sep=',', low_memory=False)
    nmdda_list = pd.read_csv(PATH_TO_NMDDA, sep=',', low_memory=False)
    qct_list = pd.read_csv(PATH_TO_QCT, sep=',', low_memory=False)
    qoz_list = pd.read_csv(PATH_TO_QOZ, sep=',', low_memory=False)

    output = input_df
    output = prepare_output(output)
    output = is_qct(output, qct_list)
    output = is_sdda(output, sdda_list)
    output = is_nmdda(output, nmdda_list)
    # output = is_qoz(output, qoz_list)

    print(output.to_string())
    output.to_csv(PATH_TO_OUTPUT)



# Get Block FIPS from Lat Long
# Inputs:
# - Latitude: as Double
# - Longitude: as Double
# Returns: Block FIPS
def get_fips(latitude, longitude):
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'censusYear': '2020',
        'showall': 'true',
        'format': 'json'
    }
    response = requests.get(url=FCC_API_PATH, params=params).json()

    response_status = response['status']
    response_time = response['executionTime']
    block_fips = response['Block']['FIPS']
    county_fips = response['County']['FIPS']

    return block_fips, county_fips, response_status, response_time


def get_fips_list(input_df, digits=11):
    lat_long_index = [LATITUDE_COL_INDEX, LONGITUDE_COL_INDEX]

    lat_long_list = input_df[lat_long_index].dropna()
    block_fips_list = []
    county_fips_list = []

    for i in range(len(lat_long_list)):
        latitude = lat_long_list.iloc[i][LATITUDE_COL_INDEX]
        longitude = lat_long_list.iloc[i][LONGITUDE_COL_INDEX]
        block_fips, county_fips, response_status, response_time = get_fips(latitude, longitude)
        block_fips = block_fips[0:digits]
        block_fips_list.append(block_fips)
        county_fips = int(county_fips)
        county_fips_list.append(county_fips)

    return block_fips_list, county_fips_list


# Inputs:
# - input_df: DataFrame of inputs with 'zip_code' as a column
# - sdda_list: DataFrame of DDAs
# Returns:
# - Pandas Series with zip code as index and and isSDDA as value
def is_sdda(input_df, sdda_list):
    # Set list to be searchable by zip codes
    sdda_list = sdda_list.set_index(ZIP_COL_INDEX)

    zip_codes = [zip_code for zip_code in input_df[ZIP_COL_INDEX].tolist() if str(zip_code) != 'nan']

    for i in range(len(zip_codes)):
        try:
            is_dda = sdda_list.loc[zip_codes[i]]['sdda_2023'] == 1
            input_df.loc[i, DDA_COL_INDEX] = (input_df.loc[i, DDA_COL_INDEX] | is_dda)
        except:
            input_df.loc[i, DDA_COL_INDEX] = (input_df.loc[i, DDA_COL_INDEX] | False)

    return input_df


# Inputs:
# - input_df: DataFrame of inputs with 'fips' as a column containing FIPS codes
# - nmdda_list: DataFrame of Non Metro DDAs
# Returns:
# - Pandas Series with FIPS code as index and and isNMDDA as value
def is_nmdda(input_df, nmdda_list):
    # Set list to be searchable by FIPS
    nmdda_list = nmdda_list.set_index(FIPS_COL_INDEX)
    block_fips_list, county_fips_list = get_fips_list(input_df)

    county_fips = [county_fips for county_fips in county_fips_list if str(county_fips) != 'nan']

    for i in range(len(county_fips)):
        try:
            is_dda = nmdda_list.loc[int(county_fips[i])]['nmdda_2023'] == 1
            input_df.loc[i, DDA_COL_INDEX] = (input_df.loc[i, DDA_COL_INDEX] | is_dda)
        except KeyError:
            input_df.loc[i, DDA_COL_INDEX] = (input_df.loc[i, DDA_COL_INDEX] | False)

    return input_df


# Inputs:
# - input_df: DataFrame of inputs with 'fips' as a column containing FIPS codes
# - QCT
# Returns:
# - Pandas Series with FIPS code as index and and isNMDDA as value
def is_qct(input_df, qct_list):
    lat_long_index = [LATITUDE_COL_INDEX, LONGITUDE_COL_INDEX]

    lat_long_list = input_df[lat_long_index].dropna()
    block_fips_list, county_fips_list = get_fips_list(input_df)

    # Set index to be searchable by FIPS
    qct_list = qct_list.set_index(BLOCK_FIPS_COL_INDEX)

    for i in range(len(lat_long_list)):
        try:
            qct_tract = qct_list.loc[int(block_fips_list[i])]['qct_id']
            input_df.loc[i, QCT_COL_INDEX] = True
        except:
            input_df.loc[i, QCT_COL_INDEX] = False

    # return qct_list.loc[block_fips_list]['qct_id']
    return input_df


# NOT RELIABLE AT ALL
def is_qoz(input_df, qoz_list):
    lat_long_index = [LATITUDE_COL_INDEX, LONGITUDE_COL_INDEX]

    lat_long_list = input_df[lat_long_index].dropna()
    block_fips_list, county_fips_list = get_fips_list(input_df, 10)

    # Select first 9 digits of the census tract
    for i in range(len(qoz_list)):
        qoz_list.loc[i, CENSUS_TRACT_COL_INDEX] = str(qoz_list[CENSUS_TRACT_COL_INDEX][i])[0:10]

    # Set index to be searchable by FIPS
    qoz_list = qoz_list.set_index(CENSUS_TRACT_COL_INDEX)

    for i in range(len(lat_long_list)):
        try:
            qoz_tract = qoz_list.loc[block_fips_list[i]]['tract_type']
            input_df.loc[i, QOZ_COL_INDEX] = True
        except:
            input_df.loc[i, QOZ_COL_INDEX] = False

    return input_df


# Add QCT, DDA, QOZ Columns if not there
def prepare_output(input_df):
    # Add QCT column if it does not exist
    try:
        x = input_df[QCT_COL_INDEX]
    except KeyError:
        input_df[QCT_COL_INDEX] = False  # Insert column with name QCT_COL_INDEX

    # Add DDA column it if does not exist
    try:
        x = input_df[DDA_COL_INDEX]
    except KeyError:
        input_df[DDA_COL_INDEX] = False  # Insert column with DDA_COL_INDEX

    # # Add QOZ column if it does not exist
    # try:
    #     x = input_df[QOZ_COL_INDEX]
    # except KeyError:
    #     input_df[QOZ_COL_INDEX] = False  # Insert column with DDA_COL_INDEX

    return input_df


if __name__ == '__main__':
    main(sys.argv[1:])
