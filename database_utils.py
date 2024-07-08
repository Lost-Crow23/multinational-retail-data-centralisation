import pandas as pd 
import yaml 
from sqlalchemy import create_engine, inspect
import psycopg2

class DatabaseConnector():
    
    def __init__(self):
        pass

    def read_db_creds(self):
        with open ("./db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)
            return creds
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        # engine = create_engine(creds)
        connec = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        cur = connec.cursor()
        return cur

    def list_db_tables(self):
        cur = self.establish.connec()    
        # with self.engine.connec() as connection:
        query = ("""SELECT table_name FROM information_schema.tables 
                 WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)
        return table
    
    def upload_to_db(self, df, table_name, yaml_file):
        with open("./db_creds_local.yaml") as db_creds_local:
            creds = yaml.safe_load(db_creds_local)
        engine = create_engine (f"{'postgresql'}+{'psycopg2'}://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}")
        engine.connect()
        df.to_sql(table_name, engine, if_exists = 'replace')
        print('Upload Done')



# if __name__ == '__main__':
#     connec = DatabaseConnector()
#     connec.read_db_creds()
#     engine = connec.init_db_engine()
#     connec.list_db_tables()


    


