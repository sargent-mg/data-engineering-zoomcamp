import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default='5432', help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', default=2021, type=int, help='Year of data to download')
@click.option('--month', default=1, type=int, help='Month of data to download')
@click.option('--table_name', default='yellow_taxi_data', help='Table name to create')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for data insertion')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, table_name, chunksize):
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    first = True

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

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
