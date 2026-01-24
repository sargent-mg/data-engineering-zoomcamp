import pandas as pd
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm
import click


taxis_dtype_map = {
    'LocationID': "Int64",
    'Borough': "string",
    'Zone': "string",
    'service_zone': "string"
}


@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default='5432', help='PostgreSQL port')
@click.option('--pg_db', default='homework', help='PostgreSQL database')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for data insertion')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, chunksize):
    prefix = './data'
    url_trip = f'{prefix}/green_tripdata_2025-11.parquet'
    url_taxi_zone = f'{prefix}/taxi_zone_lookup.csv'

    # First, create the database if it doesn't exist
    try:
        engine_admin = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/postgres')
        with engine_admin.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(f"CREATE DATABASE {pg_db};"))
        print(f"Database '{pg_db}' created successfully")
    except Exception as e:
        print(f"Database creation info: {e}")

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')


    df_parquet = pd.read_parquet(
        url_trip,
    )

    df_parquet.to_sql(name='green_taxi_data', con=engine, if_exists='replace')
    print("Inserted:", len(df_parquet))

    df_iter = pd.read_csv(
        url_taxi_zone,
        dtype=taxis_dtype_map,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    table_name = 'taxi_zone'
    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")
        # Insert chunk
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
        print("Inserted:", len(df_chunk))



if __name__ == "__main__":
    run()
