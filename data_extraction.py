from sqlalchemy.exc import SQLAlchemyError
from database_utils import DatabaseConnector as db_connector
import boto3
import pandas as pd 
import sqlalchemy as db
import tabula as tb
import requests
import json
import numpy as np
from io import BytesIO



class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_data(self, connec):
        inspector = db.inspect(connec)
        tables = inspector.get_table_names()
        print(tables)
        return tables
    
    def read_rds_table(self, db_connector, table_name):
        print("running the read_rds_table")
        engine = db_connector.init_db_engine()
        query = (f"SELECT * FROM  {table_name}")
        data = pd.read_sql_table(query, engine)
        df = pd.DataFrame(data)
        print("done")
        return df 
    
    def retrieve_pdf_data(self):
        print("retrieving all the data...")
        link = "https://data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        card_details = pd.concat(tb.read_pdf(link, pages = 'all'), ignore_index = True)
        print("done")
        return (card_details)

    def list_number_of_stores(self, my_endpoint, api_key):
        print("running list_number_of_stores")
        response = requests.get(my_endpoint, headers = api_key).content
        result = json.loads(response)
        result = result['number_of_stores']
        print("done")
        result(result)

    def retrieve_stores_data(self, endpoint, num_stores, api_key):
        print("running retreive_stores_data")
        store_details = []
        for stores in range(0, int(num_stores)):
            response = requests.get(f"{endpoint}{stores}", headers = api_key).json()
            store_details.append(pd.DataFrame(response, index = [np.NaN]))
        store_details_df = pd.concat(store_details)
        print("done")
        return (store_details_df)
    
    def extract_from_s3(self, address):
        print("running extract from s3")
        client = boto3.client('s3')
        if "s3://" in address:
             split_address = address.replace("s3://", "").split("/", 1)
        elif "https://" in address:
             split_address = address.replace("https://", "").split("/", 1)
        bucket_name = 'data-handling-public'
        s3key_path = "/".join(split_address[1:])
        data = client.get_object(Bucket = bucket_name, Key = s3key_path)
        data = data['Body'].read()

        if 'csv' in s3key_path:
            df = pd.read_csv(BytesIO(data))
        elif 'json' in s3key_path:
            df = pd.read_json(BytesIO(data))
        print('done')
        return df