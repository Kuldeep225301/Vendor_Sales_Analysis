import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log", 
    level=logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe and  into db table'''
    df.to_sql(table_name, con = engine, if_exists= 'replace', index = False)

def load_raw_data():
    '''This function will load the csv and dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            logging.info(f"Ingesting{file} in db ")
            for chunk in pd.read_csv(f"data/{file}", chunksize=100000):
                chunk.to_sql(file[:-4],engine, if_exists='append', index=False)
    end= time.time()
    total_time = (end - start)/60
    logging.info('__________________Ingestion Complete______________________________')
    
    logging.info(f'Total time taken: {total_time} mintues ')

if __name__ == '__main__':
    load_raw_data()