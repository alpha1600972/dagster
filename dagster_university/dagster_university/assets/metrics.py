from dagster import asset, AssetExecutionContext

from dagster_duckdb import DuckDBResource
from ..partitions import weekly_partition

import matplotlib.pyplot as plt
import duckdb
import os
import geopandas as gpd

from . import constants

@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    """
    Calcule les statistiques des trajets en taxi pour Manhattan et les stocke au format GeoJSON.
    """
    query = """
        SELECT
        zones.zone,
        zones.borough,
        zones.geometry,
        COUNT(1) AS num_trips
        FROM trips
        LEFT JOIN zones ON trips.pickup_zone_id = zones.location_id
        WHERE zones.borough = 'Manhattan' AND zones.geometry IS NOT NULL
        GROUP BY zones.zone, zones.borough, zones.geometry
    """
    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()
    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone, geometry="geometry")
    
    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


@asset(
    deps=["manhattan_stats"]
)
def manhattan_map() -> None:
    """
    Génère une carte des trajets en taxi à Manhattan et l'enregistre sous forme d'image.
    """
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    if trips_by_zone.empty:
        raise Exception("Dataset vide")

    fig, ax = plt.subplots(figsize=(10, 10))

    trips_by_zone.plot(
        column="num_trips",
        cmap="plasma",
        legend=True,
        ax=ax,
        edgecolor="black"
    )

    ax.set_title("Nombre de trajets par zone de taxi à Manhattan")

    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    Calcule le nombre de trajets en taxi par semaine et les stocke au format CSV.
    """
    partition_date_str = context.partition_key
    query = f"""
        SELECT
        DATE '{partition_date_str}' AS period,
        COUNT(1) AS num_trips,
        SUM(passenger_count) as passenger_count,
        SUM(total_amount) as total_amount,
        SUM(trip_distance) as trip_distance
        fROM trips
        WHERE  DATE_TRUNC('week', pickup_datetime) + INTERVAL '6 days' = DATE '{partition_date_str}'
        GROUP BY period
        ORDER BY period
    """
    with database.get_connection() as conn:
        trips_by_week = conn.execute(query).fetch_df()
    conn.close()
    trips_by_week.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)


