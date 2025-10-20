import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


DB_NAME = 'inventory.db'
DATA_DIR = 'data'
engine = create_engine(f'sqlite:///{DB_NAME}')

def ingest_db(df, table_name, engine):
    '''This function ingests the DataFrame into the database table using batching.'''
    logging.info(f'Inserting {len(df)} rows into table {table_name}')
    try:
        # Crucial for performance: insert in batches of 50,000 rows
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=50000)
    except Exception as e:
        logging.error(f'Failed to ingest {table_name}: {e}')

def load_raw_data():
    '''This function loads all CSVs as DataFrames and ingests them into the DB.'''
    start_time = time.time()
    if not os.path.isdir(DATA_DIR):
        logging.error(f"Directory not found: {DATA_DIR}. Cannot load data.")
        return
    for file in os.listdir(DATA_DIR):
        if file.endswith('.csv'):
            table_name = file[:-4]
            file_path = os.path.join(DATA_DIR, file)

            try:
                logging.info(f'Loading file: {file}')
                df = pd.read_csv(file_path)
                ingest_db(df, table_name, engine)

            except Exception as e:
                logging.error(f'Error processing {file_path}: {e}')

    end_time = time.time()
    total_time_minutes = (end_time - start_time) / 60

    logging.info('='*30)
    logging.info('ALL INGESTION COMPLETE')
    logging.info(f'Total time taken: {total_time_minutes:.2f} minutes')
    logging.info('='*30)

if __name__ == '__main__':
    load_raw_data()