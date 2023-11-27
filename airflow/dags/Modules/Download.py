# importing the libraries
import wget
from datetime import date, timedelta, datetime
from pathlib import Path

# the data is a little werid, since we only have the taxis data from the last 3 months, it was tricky to handle it
# since it is tricky and I can't guarantee the date will always be minux 3 months, I'm getting the data of the last 4 months

# this function will get the dates of the links
def get_dates_of_data_files(starting_year, starting_month):
    current_year = datetime.now().year
    current_month = datetime.now().month
    if(current_month<5):
        start_dt = date(starting_year, starting_month, 1)
        end_dt = date(current_year-1, current_month+8, 30)
    else:
        start_dt = date(starting_year, starting_month, 1)
        end_dt = date(current_year, current_month-4, 30)
    # difference between current and previous date
    delta = timedelta(days=28)
    
    # store the dates between two dates in a list
    dates = []
    while start_dt <= end_dt:
        # add current date to list by converting  it to iso format
        dates.append(start_dt.isoformat())
        # increment start date by timedelta
        start_dt += delta
    
    newdate = []
    for i in dates:
        newdate.append(i[:-3])
    all_date = list(dict.fromkeys(newdate).keys())
    return all_date

# this function will get us the links based on the dates that we got from the previous function
def get_links_of_data(taxi_type):
    taxis_list = []
    dates = []
    if(taxi_type=='yellow'):
        dates = get_dates_of_data_files(2023,1)
    elif(taxi_type=='green'):
        dates = get_dates_of_data_files(2023,1)
    elif(taxi_type=='fhv'):
        dates = get_dates_of_data_files(2023,1)
    elif(taxi_type=='fhvhv'):
        dates = get_dates_of_data_files(2023,1)
    else:
        return "not a valid taxi type"
    for i in dates:
        link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{i}.parquet"
        taxis_list.append(link)
    return taxis_list

# if all files exist it will return nothing, if a file is missing it will return its name
def check_if_files_exist():
    yellow= get_links_of_data('yellow')
    green= get_links_of_data('green')
    fhv= get_links_of_data('fhv')
    fhvhv= get_links_of_data('fhvhv')
    all_urls = yellow + green + fhv + fhvhv
    list_of_missing_files = []
    for i in all_urls:
        path = Path(f'./data/{i[48:]}')
        if(path.is_file()):
            pass
        else:
            list_of_missing_files.append(path.name)
    return list_of_missing_files

def download_the_data():
    list_of_missing_files = check_if_files_exist()
    list_of_links_to_download = []
    for i in list_of_missing_files:
        link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{i}"
        list_of_links_to_download.append(link)
    for i in list_of_links_to_download:
        wget.download(i, 'data')