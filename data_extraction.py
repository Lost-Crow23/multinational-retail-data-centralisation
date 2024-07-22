from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from database_utils import DatabaseConnector as db_con
import boto3
import pandas as pd 
import sqlalchemy as db
import tabula as tb
import requests
from io import BytesIO
import os
import json
from dotenv import load_dotenv

class DataExtractor():
    def __init__(self):
        pass
        return 
    """
    This class produces the methods for extracting and retrieving data from a relational database, PDF documents, 
    API ednpoints, and via the Amazon s3 storage.

    Attributes:
        db_con (DatabaseConnector): Instance of the DatabaseConnector class for database connection.
    
    Public methods for extracting:
    1. read_data(connec)
            - Reads the data and retreives all the tables in the connected database. 
            - Parameters:
                - connec(data) which represents a database connection.
            Returns:
                - prints out the list table names to the caller method.
    
    2. read_dbs_table(db_con, table_name)
            - Extracts data from a specified table in the connected database.
            - Parameters:
                - db_con (DatabaseConnector): Connection to database
                - table_name (str): Name of the tabe in the extract data from.
            - Returns:
                DataFrame: Extracted data from the specified table.
    
    3. retrieve_pdf_data()
            - Extracts data from a PDF document located at the specified table.
            - Parameters:
                - Takes None: Link (str) is associated (URL of the PDF document)
            - Returns:
                - DataFrame: Extracted data from the PDF.
    
    4. list_number_of_stores(endpoint, api_key)
            - Retrieves the number of stores from an API endpoint.
            - Parameters:
                - endpoint (str): API endpoint for getting the number of stores.
                - api_key (dict): Headers of the API request.
            - Returns:
                - int: Number of stores
    
    5. retrieve_stores_data(store_endpoint, api_key)
            - Retrieves the store data from an API endpoint for a specified number of stores.
            - Parameters:
                - store_endpoint (str): API endpoint for getting the store data.
                - api_key (dict): Headers for the API request.
            - Returns:
                - DataFrame: Extracted store data.

    6. extract_from_s3(self, address)
            - Extracts and retrieves the data from an Amazon s3 storage / Extracts the JSON data
            - Parameters:
                - address (str): Address of the data file in Amazon s3.
            - Returns:
                - DataFrame: Extracted data
                - DataFrame: Extracted JSON data

    Note:
        To use this class, a passing of an instance of the DatabaseConnector class has be done when initialising DataExtractor.
    """ 

# 1. Inspects the connection of the table names
    def read_data(self, connec):
        inspector = db.inspect(connec)
        tables = inspector.get_table_names()
        print(tables)
        return tables
    
# 2. Read user_data data from the RDS table
    def read_rds_table(self, db_con, table_name):
        print("running the read_rds_table")
        try:
            data = pd.read_sql_table(table_name, db_con)
            df = pd.DataFrame(data)
            print("done")
            return df
        except SQLAlchemyError as e:
            print(f"Error reading table '{table_name}': {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occured: {e}")
            return None
    
# 3. Read card_data data from the RDS table 
    def retrieve_pdf_data(self):
        print("retrieving all the data...")
        try:
            link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
            card_details = tb.read_pdf(link, pages='all')
            card_details = pd.concat(card_details)
            print("done")
            return (card_details)
        except Exception as e:
            print(f"Error extracting data form PDF: {str(e)}")

# 4. Get the number_of_stores
    def list_number_of_stores(self, endpoint, api_key ):
        api_key = os.getenv('x-api-key')
        endpoint = os.getenv("number_of_stores_endpoint")
        print("running list_number_of_stores")
        try:
            response = requests.get(endpoint, headers = {"x-api-key": api_key}).content
        except requests.RequestException as e:
            print(f"Error not making API reqeust: {e}")
        except Exception as e:
            print(f"Error unexpectedly occured: {e}")
            return None
        data = json.loads(response)
        number_of_stores = data['number_stores']
        print(number_of_stores)
        print("done")
        return number_of_stores
    
# 5. Get the stores data
    def retrieve_stores_data(self, store_endpoint, api_key):
        store_endpoint = os.getenv('store_details_endpoint')
        api_key = os.getenv('x-api-key')
        print("running retrieve_stores_data...")
        stores_data = []
        for store_number in range(0, 451):
            stores_url = store_endpoint.format(store_number = store_number)
            response = requests.get(stores_url, headers = {"x-api-key": api_key})
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
        store_details_df = pd.DataFrame(stores_data)
        print("done")
        return (store_details_df)
    
# 6. Extracts the data from Amazon s3
    def extract_from_s3(self, address):
        print("running extract from s3...")
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

# Calls out the function
if __name__ == "__main__":
    DataExtractor()  

