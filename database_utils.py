import pandas as pd 
import yaml 
from sqlalchemy import create_engine

class DatabaseConnector():
    
    def read_db_creds(self):
        with open ("./db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)
            return creds
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        # engine = create_engine(creds)
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine 


connec = DatabaseConnector()
connec.read_db_creds()


    


