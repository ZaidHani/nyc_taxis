# this file is runned only once unless you want your data to be duplicated

# importing libraries
from ETL import etl
from Download import download_the_data

download_the_data()
etl('yello')
etl('green')
etl('fhv')
etl('fhvhv')