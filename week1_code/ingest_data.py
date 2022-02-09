import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
import subprocess

def download_data(params):
    name=params.filename   
    subprocess.call(["wget",params.url,"-O",name])



def send_to_postgres(params):
    data_iter=pd.read_csv(params.filename,iterator=True,chunksize=100000,parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])    
    engine=create_engine('postgresql://{}:{}@{}:{}/{}'.format(params.user,params.password,params.host,params.port,params.db))

    while True:
        t_init=time()
        try:
            data_temp=next(data_iter)
            data_temp.to_sql(name=params.table_name,con=engine,if_exists="append")
            t_end=time()
            print("uploaded batch, took {:.2f} seconds".format(t_end-t_init))
        except StopIteration:
            print("all rows uploaded")
            break
        

if __name__=="__main__":
    
    parser = argparse.ArgumentParser(description="Taxi to Postgres")
    parser.add_argument("--url", help="url for the file to download ", dest="url")
    parser.add_argument("--filename", help="filename for csv file to be read ", dest="filename")
    parser.add_argument("--table", help="table name to use to upload csv file in postgres", dest="table_name")
    parser.add_argument("--user", help="postgres user", dest="user")
    parser.add_argument("--password", help="postgres password ", dest="password")
    parser.add_argument("--host",help="host ip database",dest="host")
    parser.add_argument("--db", help="postgres database", dest="db")
    parser.add_argument("--port", help="postgres port", dest="port",type=int)
    args = parser.parse_args()

    download_data(args)
    send_to_postgres(args)