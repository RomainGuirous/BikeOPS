import os
from pyspark.sql import SparkSession, functions as F

from etl.silver.availability_silver import create_silver_availability_df

if __name__ == "__main__":
    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # lecture du fichier CSV availability_raw
    df_availability = create_silver_availability_df()[0]

    # extraction colonnes station_id et capacity depuis stations.csv
    df_stations = read_csv_spark(
        spark,
        "/app/data/data_raw/stations.csv",
        ["station_id", "capacity"],
        delimiter=",",
    )

    df_weather = read_csv_spark(
        spark,
        "/app/data/data_raw/weather.csv",
        ["date", "temperature", "precipitation"],
        delimiter=",",
    )