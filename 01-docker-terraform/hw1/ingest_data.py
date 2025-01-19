from time import time
from sqlalchemy import create_engine
import pandas as pd
import argparse
import os
import io
#import requests

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #csv_name = url.split("/")[-1]
    csv_name = 'output.csv'

    #url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"



    # Step 2: Raise an exception if there's an issue with the download
    #response.raise_for_status()

    # Step 3: Load the gzipped CSV content directly into Pandas
    #df_iter = pd.read_csv(io.BytesIO(response.content), compression="gzip", iterator=True, chunksize=100000)


    #csv_gz_name = url.split("/")[-1]  # This will be "yellow_tripdata_2021-01.csv.gz"
    #csv_name = csv_gz_name.replace(".gz", "")

    # download the csv
    # os system function can run command line arguments from Python
    os.system(f"wget {url} -O {csv_name}")

    #csv_name = "yellow_tripdata_2021-01.csv"

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, compression="gzip", iterator=True, chunksize=100000)
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #  adding the column names
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    # adding the first batch of rows
    df.to_sql(name=table_name, con=engine, if_exists="append")
    
    while True:
        try:
            t_start = time()
            df = next(df_iter)
            
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv

    parser.add_argument('--user', help="user name for postgres")
    parser.add_argument('--password', help="password for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="database name for postgres")
    parser.add_argument('--table_name', help="name of the table where we will write the results to")
    parser.add_argument('--url', help="url of the CSV")

    args = parser.parse_args()

    # xprint(args.accumulate(args.integers))

    main(args)
