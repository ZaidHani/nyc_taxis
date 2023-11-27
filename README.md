# nyc taxis data pipeline
## This is the code that I used to create an ETL pipeline on the NYC taxis data
this code works first by downloading the historical data from [t](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page and after it is processed we will start the pipeline so it can process this data every month, you need to start your own airflow instance and your own python environment before you could start the pipeline.
the data will always be 4 months old for the sake of convenience, since the nyc.gov website updates its data every 3 months.
