import pandas as pd
import urllib3
import certifi
import json
import requests
import sqlite3
from urllib3 import request
from unicodedata  import normalize

def source_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv()
    except:
        df_csv = pd.Dataframe()
    return df_csv

def source_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet()
    except:
        df_parquet = pd.Dataframe()
    return df_parquet

def source_from_api(api_endpoint):
    try:
        http = urllib3.PoolManager(cert_reqs='CERT_REQUESTED',ca_certs=certifi.where())
        api_response = http.request('GET',api_endpoint)
        api_status = api_response.status
        print(api_status)
        
        if api_status == 200:
            data = json.loads(api_response.request.data.decode('utf-8'))
            df_api = pd.json_normalize(data)
        else:
            df_api = pd.Dataframe()
    except Exception as e:
        return df_api.head(10)
    
def source_from_database(db_name,table_name):
    try:
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql("SELECT * FROM {table_name}", conn)
    except Exception as e:
        df_table = pd.Dataframe()
    return df_table

def source_from_web(web_page_url,matching_keyword):
    try:
        df_html = pd.read_html(web_page_url,match=matching_keyword)
        df_html = df_html[0]
    except Exception as e:
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