import pandas as pd
import urllib3
import certifi
import json
import requests
import sqlite3
import logging

logger = logging.getLogger(__name__)

def source_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv()
        logger.info(f'{csv_file_name}:extracted {df_csv.shape[0]} record from the csbbv file')
    except Exception as e:
        logger.info(f'{csv_file_name}-exception {e} encountered while extracting csv file')
        df_csv = pd.Dataframe()
    return df_csv

def source_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet()
        logger.info(f'{parquet_file_name} extracted {df_parquet.shape[0]} records from the parquet file')
    except:
        df_parquet = pd.Dataframe()
    return df_parquet

def source_from_api(api_endpoint):
    try:
        http = urllib3.PoolManager(cert_reqs='CERT_REQUESTED',ca_certs=certifi.where())
        api_response = http.request('GET',api_endpoint)
        apt_status = api_response.status
        print(apt_status)
        
        if apt_status == 200:
            logger.info(f'{apt_status}- ok while invoking the api {api_endpoint}')
            data = json.loads(api_response.request.data.decode('utf-8'))
            df_api = pd.json_normalize(data)
            logger.info(f'{apt_status} extracted {df_api.shape[0]}')
        else:
            logger.error(f'{apt_status}-error while invoking the {api_endpoint}')
            df_api = pd.Dataframe()
    except Exception as e:
        return df_api.head(10)
    
def source_from_database(db_name,table_name):
    try:
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql("SELECT * FROM {table_name}", conn)
            logger.info(f'{db_name}-read {df_table.shape[0]} from the table: {table_name}')
    except Exception as e:
        logger.exception(f'{db_name}:- exception {e} encountered while reading data from the table: {table_name}')
        df_table = pd.Dataframe()
    return df_table

def source_from_web(web_page_url,matching_keyword):
    try:
        df_html = pd.read_html(web_page_url,match=matching_keyword)
        df_html = df_html[0]
        logger.info(f'{web_page_url}- read {df_html.shape[0]} records from the page {web_page_url}')
    except Exception as e:
        logger.exception(f'{web_page_url}: exception {e} encountered from the data page: {web_page_url}')
        df_html = pd.Dataframe()
    return df_html

def extracted_name():
    csv_file_name = "h9gi-nx95.csv"
    parquet_file_name = "yellow_tripdata_2022-01.parquet"
    api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
    web_page_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
    db_name = "movies.sqlite"
    table_name = "movies"
    matching_keyword = "by country"

    df_csv,df_parquet,df_database,df_html,df_api = (source_from_csv(csv_file_name),
                                                 source_from_parquet(parquet_file_name),
                                                 source_from_api(api_endpoint),
                                                 source_from_database(db_name,table_name),
                                                 source_from_web(web_page_url,matching_keyword))
    
    return df_csv,df_parquet,df_api,df_database,df_html