# importing necessary libraries and modules
import polars as pl
import datetime as dt
import Download
from pathlib import Path
import numpy as np
import os
from sqlalchemy import create_engine
import time

# creating some necssary variables and lists
yellow_columns = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",]

green_columns = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "store_and_fwd_flag",
    "ratecode_id",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "trip_type",
    "congestion_surcharge",]

fhv_columns = [
    "dispatching_base_num",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "shared_ride",
    "affiliated_base_number",]

fhvhv_columns = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag",]

yellow_types = [pl.Int8, pl.Datetime, pl.Datetime, pl.Int8, pl.Float32,
                pl.Int8, pl.Utf8, pl.Int16, pl.Int16, pl.Int8,
                pl.Float32, pl.Float32, pl.Float32, pl.Float32, pl.Float32, 
                pl.Float32, pl.Float32, pl.Float32, pl.Float32, pl.Duration,
                pl.Int64]

green_types = [pl.Int8, pl.Datetime, pl.Datetime, pl.Utf8, pl.Int8,
               pl.Int16, pl.Int16, pl.Int8, pl.Float32, pl.Float32,
               pl.Float32, pl.Float32, pl.Float32, pl.Float32, pl.Float32,
               pl.Float32, pl.Float32, pl.Int8, pl.Int8, pl.Float32,
               pl.Duration, pl.Int64]

fhv_types = [pl.Utf8, pl.Datetime, pl.Datetime, pl.Int16, pl.Int16,
               pl.Int8, pl.Utf8, pl.Duration, pl.Int64]

fhvhv_types = [pl.Utf8, pl.Utf8, pl.Utf8, pl.Datetime, pl.Datetime, 
                pl.Datetime, pl.Datetime, pl.Int16, pl.Int16, pl.Float32,
                pl.Int32, pl.Float32, pl.Float32, pl.Float32, pl.Float32,
                pl.Float32, pl.Float32, pl.Float32, pl.Float32, pl.Utf8,
                pl.Utf8, pl.Utf8, pl.Utf8, pl.Utf8]

green_nulls_to_fill = ['ehail_fee']
fhv_nulls_to_fill = ['shared_ride']


# function for reading the parquet files
def read_the_file(file):
    df = pl.read_parquet(file)
    print(f'extracting the file {file} is done!')
    return df

# putting the files in a list so we can continue our process
def check_for_parquet_files(type):
    list = []
    try:
        # there is a function in the Extract.py file that has a useful function for us to get the file names using their links
        listOfFiles = Download.get_links_of_data(type)
        for file in listOfFiles:
            path = Path(f'data/{file[48:]}')
            if(path.is_file()):
                list.append(path)
    except Exception:
        pass
    return list

# we used the preivous function to check the parquet files and now we will store them in a list
def read_parquet_files(type):
    list_of_readable_files = check_for_parquet_files(type)
    actual_files = []
    for i in list_of_readable_files:
        actual_files.append(str(i).replace('\\','/'))
    return actual_files

# renaming some of the columns to make the transformation phase easier
def rename_columns(df,renamed_columns):
    for i in range(len(df.columns)):
        df = df.rename({df.columns[i]: renamed_columns[i]})
    return df

# adding 2 time columns one for the total time in nano-seconds and the other in a duration data type
def add_time_columns(df):
    df = df.with_columns(
        time_of_the_trip = pl.col('dropoff_datetime') - pl.col('pickup_datetime')
    )
    df = df.with_columns([
        pl.col("time_of_the_trip").cast(pl.Int64).alias("time_in_ns")
    ])
    return df

# a fucntion to delete the outliers
def delete_outliers(df, columns):
    for column in columns:
        q3, q1 = np.percentile(df[column], [75, 25])
        iqr = q3 - q1
        df = df.filter(
            (pl.col(column) >= (q1-1.5*iqr)) & (pl.col(column) <= (q3+1.5*iqr))
        )
    return df

# this function fills the null values in certain columns with 0
def fill_nulls_with_zero(df, columns):
    for column in columns:
        df = df.with_columns(
            pl.col(column).fill_null(0).alias(column)
        )
    return df

# this function changes the data types of the columns
def change_data_types(df, types):
    for i in range(len(df.columns)):
        df = df.with_columns(
            pl.col(df.columns[i]).cast(types[i], strict=False).alias(df.columns[i])
        )
    return df

# this function stores the dataframe depending on the type inside of the PostgreSQL database
def store_in_postgresql(df, type):
    password = 'anon'
    username = 'postgres'
    database = 'nyc_taxis'
    uri = f'postgresql://{username}:{password}@localhost:5432/{database}'
    engine = create_engine(uri)
    common_sql_state = "SQLSTATE: 42P07"
    
    if type=='yellow':
        table_name = 'yellow_taxi_trips'
    elif type=='green':
        table_name = 'green_taxi_trips'
    elif type=='fhv':
        table_name = 'fhv_taxi_trips'
    elif type=='fhvhv':
        table_name = 'fhvhv_taxi_trips'
    else:
        print('invalid type')
    
    try:
        df.write_database(table_name, connection=uri,
                          engine='adbc', if_exists='replace')
    except Exception as e:
        if(common_sql_state in str(e)):
            df.write_database(table_name, connection=uri,
                          engine='adbc', if_exists='append')
        else:
            print(e)

# this function specifically stores the fhvhv taxi data by chunking the dataframes into smaller dataframes and storing them one by one
def load_fhvhv_by_chunking(df, num_of_chunks):
    try:
        length = len(df) // num_of_chunks
        j = 0
        list_of_chunks = []
        
        for i in range(num_of_chunks):
            list_of_chunks.append(j)
            j += length
        list_of_chunks.append(len(df))
                
        for i in range(num_of_chunks):
            sliced_df = df.slice(list_of_chunks[i], list_of_chunks[i+1] - list_of_chunks[i])
            store_in_postgresql(sliced_df, 'fhvhv')
    except Exception as e:
        print(e)

# this is the result of all the previous functions
def etl(type):
    list_of_files = read_parquet_files(type)
    filtering_columns = []

    if(type=='yellow'):
        filtering_columns = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'time_in_ns']
        for file in list_of_files:
            df = read_the_file(file)
            df = rename_columns(df, yellow_columns)
            df = add_time_columns(df)
            df = df.drop_nulls()
            df = change_data_types(df, yellow_types)
            df = delete_outliers(df, filtering_columns)
            print(f'transforming has been completed!')
            store_in_postgresql(df, type)
            os.remove(file)
            print('the loading has been completed!')
        print('The ETL process for the yellow taxi data has been completed!')
        
    elif(type=='green'): 
        filtering_columns = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount', 'time_in_ns']
        for file in list_of_files:
            df = read_the_file(file)
            df = rename_columns(df, green_columns)
            df = add_time_columns(df)
            df = fill_nulls_with_zero(df, green_nulls_to_fill)
            df = df.drop_nulls()
            df = change_data_types(df, green_types)
            df = delete_outliers(df, filtering_columns)
            print(f'transforming has been completed!')
            store_in_postgresql(df, type)
            os.remove(file)
            print('the loading has been completed!')
        print('The ETL process for the green taxi data has been completed!')
        
    elif(type=='fhv'):
        for file in list_of_files:
            df = read_the_file(file)
            df = rename_columns(df, fhv_columns)
            df = add_time_columns(df)
            df = change_data_types(df, fhv_types)
            df = fill_nulls_with_zero(df, fhv_nulls_to_fill)
            print('transforming has been completed!')
            store_in_postgresql(df, type)
            os.remove(file)
            print('the loading has been completed!')
        print('The ETL process for the for hire vehicle (fhv) taxi data has been completed!')
        
    elif(type=='fhvhv'):
        filtering_columns = ['trip_miles', 'trip_time', 'base_passenger_fare', 'sales_tax', 'tips', 'driver_pay']
        for file in list_of_files:
            df = read_the_file(file)
            df = rename_columns(df, fhvhv_columns)
            df = change_data_types(df, fhvhv_types)
            df = delete_outliers(df, filtering_columns)
            print('transforming has been completed!')
            load_fhvhv_by_chunking(df, 10)
            os.remove(file)
            print('the loading has been completed!')
        print('The ETL process for the high volume for hire vehicle (fhvhv) taxi data has been completed!')
        
    else:
        print('invalid type')