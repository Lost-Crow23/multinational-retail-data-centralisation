import pandas as pd 
import yaml 
from sqlalchemy import create_engine, text, inspect
import psycopg2
from dotenv import load_dotenv, dotenv_values
import os

class DatabaseConnector():
    
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
    def init_db_engine(self):
        # creds = self.read_db_creds()
        # engine = create_engine(creds)
        # connec = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        engine = create_engine(f"postgresql+psycopg2://{self.RDS_USER}:{self.RDS_PASSWORD}@{self.RDS_HOST}:{self.RDS_PORT}/{self.RDS_DATABASE}")
        return engine

    def list_db_tables(self):
        cur = self.establish.connec()    
        # with self.engine.connec() as connection:
        query = ("""SELECT table_name FROM information_schema.tables 
                 WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)
        return table
    
    def upload_to_db(self, df, table_name):
        # with open("./db_creds_local.yaml") as db_creds_local:
        #     creds = yaml.safe_load(db_creds_local)
        loca_dburi = (f"{'postgresql'}://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_DATABASE}")
        local_engine = create_engine(loca_dburi)
        local_connec = local_engine.connect()
        df.to_sql(table_name, local_connec, if_exists = 'replace')
        print('Upload Done')



# if __name__ == '__main__':
#     connec = DatabaseConnector()
#     connec.read_db_creds()
#     engine = connec.init_db_engine()
#     connec.list_db_tables()


    


