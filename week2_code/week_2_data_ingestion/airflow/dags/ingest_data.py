import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
import subprocess

def send_to_postgres(filename,user,password,host,port,db,table_name):
    print(filename,table_name)
    data_iter=pd.read_csv(filename,iterator=True,chunksize=100000,parse_dates=['tpep_pickup_datetime','tpep_dropoff_datetime'])    
    engine=create_engine('postgresql://{}:{}@{}:{}/{}'.format( user, password, host, port, db))

    while True:
        t_init=time()
        try:
            data_temp=next(data_iter)
            data_temp.to_sql(name= table_name,con=engine,if_exists="append")
            t_end=time()
            print("uploaded batch, took {:.2f} seconds".format(t_end-t_init))
        except StopIteration:
            print("all rows uploaded")
            break
        