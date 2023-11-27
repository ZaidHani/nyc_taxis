import Download
import wget
from pathlib import Path

# this will return a function of files that are 4 months old
def check_if_recent_files_exist():
    yellow = Download.get_links_of_data('yellow')
    green = Download.get_links_of_data('green')
    fhv = Download.get_links_of_data('fhv')
    fhvhv = Download.get_links_of_data('fhvhv')
    all_urls = [yellow[-1]] + [green[-1]] + [fhv[-1]] + [fhvhv[-1]]
    list_of_missing_files = []
    for i in all_urls:
        path = Path(f'./data/{i[48:]}')
        if(path.is_file()):
            pass
        else:
            list_of_missing_files.append(path.name)
    return list_of_missing_files

# this will download the 4 files that are 4 months old
def download_batchly():
    list_of_missing_files = check_if_recent_files_exist()
    list_of_links_to_download = []
    for i in list_of_missing_files:
        link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{i}"
        list_of_links_to_download.append(link)
    for i in list_of_links_to_download:
        wget.download(i, 'data')