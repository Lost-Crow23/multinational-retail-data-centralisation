import pandas as pd 
import yaml 
from sqlalchemy import create_engine, text, inspect
import psycopg2
from dotenv import load_dotenv, dotenv_values
import os

# def main():
class DatabaseConnector():
    """
    This class produces the methods for connecting to a PostgreSQL database and performing database-related tasks.

    Attributes:
        .env file (dotenv) (dict) : A dictitonary containing database credentials.
        engine (sqlalchemy.engine.base.Engine): A SQLAlchemy database engine used for database connections.
    
    Methods:

    1. init_db_engine()
        - Initializes a SQLAlchemy database engine using the database credentials.
        - Returns:
            - sqlachemy.engine.base.Engine: A SQLAlchemy database engine.
    
    2. list_db_tables()
        - Lists all the tables in the connected PostgreSQL database
        - Returns:
            - list: A list of tables names in the 'public' schema of the database.
    
    3. upload_to_db(df, table_name)
        - Uploads data from a pandas DataFrame to a specified database table.
        - Parameters:
            - df (DataFrame): The data to be uploaded
            - table_name (str): The name of the tabe in the database
    
    4. close_connection()
        - Closes the database connection if it's open.
    
Usage:
    Example usage of this class can be found in the '__main__' block of this script
    """
    def __init__(self):
        load_dotenv('.env')
        # External credentials 
        self.RDS_HOST = os.getenv('RDS_HOST')
        self.RDS_PASSWORD = os.getenv('RDS_PASSWORD')
        self.RDS_USER = os.getenv('RDS_USER')
        self.RDS_DATABASE = os.getenv('RDS_DATABASE')
        self.RDS_PORT = os.getenv("RDS_PORT")
        # local credentials 
        self.DB_HOST = os.getenv('DB_HOST')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD')
        self.DB_USER = os.getenv('DB_USER')
        self.DB_DATABASE = os.getenv('DB_DATABASE')
        self.DB_PORT = os.getenv('DB_PORT')

    # def read_db_creds(self):
    #     with open ("./db_creds.yaml", "r") as stream:
    #         creds = yaml.safe_load(stream)
    #         return creds

# 1. Iniliases the database engine
    def init_db_engine(self):
        # creds = self.read_db_creds()
        # engine = create_engine(creds)
        # connec = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        engine = create_engine(f"postgresql+psycopg2://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DATABASE}")
        return engine
    
# 2. Lists all the tables
    def list_db_tables(self):
        cur = self.establish.connec()    
        query = ("""SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)
        return table
    
# 3. Uploads all the modified dataframes to a local connection
    def upload_to_db(self, df, table_name):
        # with open("./db_creds_local.yaml") as db_creds_local:
        #     creds = yaml.safe_load(db_creds_local)
        loca_dburi = (f"{'postgresql'}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DATABASE}")
        local_engine = create_engine(loca_dburi)
        local_connec = local_engine.connect()
        df.to_sql(table_name, local_connec, if_exists = 'replace')
        print('Upload Done')

# 4. Closes the connection if it is open
    def close_connection(self):
        if hasattr(str, 'con'):
            self.engine.dispose()
            print("Database connection closed")

    # Calls out the function
if __name__ == "__main__":
    DatabaseConnector()


    


