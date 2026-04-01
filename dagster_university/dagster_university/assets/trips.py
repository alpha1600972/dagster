from dagster import asset, AssetExecutionContext
from dagster.utils.backoff import backoff
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition
from ..resources import database_resource

import duckdb
import os
import requests
from . import constants

@asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
    Récupère les fichiers Parquet bruts des trajets en taxi.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3] 
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
        )
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

@asset
def taxi_zones_file() -> None:
    """
    Récupère le fichier Parquet brut des zones de taxi.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
        )
    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)


@asset(
deps=["taxi_trips_file"],
partitions_def=monthly_partition
)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    Le jeu de données brut des trajets en taxi, chargé dans une base de données DuckDB.
    """
    partition_date_str = context.partition_key
    month_to_load = partition_date_str[:-3]
    file_path = f"data/raw/taxi_trips_{month_to_load}.parquet"

    query =create_query = """
        CREATE TABLE IF NOT EXISTS trips (
            vendor_id INTEGER,
            pickup_zone_id INTEGER,
            dropoff_zone_id INTEGER,
            rate_code_id DOUBLE,
            payment_type INTEGER,
            dropoff_datetime TIMESTAMP,
            pickup_datetime TIMESTAMP,
            trip_distance DOUBLE,
            passenger_count INTEGER,
            total_amount DOUBLE,
            partition_date DATE
        );
    """

    insert_query = f"""
        INSERT INTO trips
        SELECT
            VendorID,
            PULocationID,
            DOLocationID,
            RatecodeID,
            payment_type,
            tpep_dropoff_datetime,
            tpep_pickup_datetime,
            trip_distance,
            passenger_count,
            total_amount,
            DATE '{partition_date_str}' as partition_date
        FROM {file_path}
    );
    """
    with database.get_connection() as conn:
        conn.execute(query)
    conn.close()




@asset(
    deps=["taxi_zones_file"]
    )
def taxi_zones(database: DuckDBResource) -> None:
    """
    Le jeu de données brut des zones de taxi, chargé dans une base de données DuckDB.
    """
    query = """
        CREATE OR REPLACE TABLE zones AS (
        SELECT LocationID AS location_id,
        Shape_Leng AS shape_length,
        Zone AS zone,
        borough AS borough,
        the_geom AS geometry,
        Shape_Area AS shape_area
        FROM 'data/raw/taxi_zones.csv'
    );
    """
    with database.get_connection() as conn:
        conn.execute(query)
    
       