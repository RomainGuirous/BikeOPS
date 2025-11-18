from pyspark.sql import SparkSession, functions as F

from etl.silver.availability_silver import create_silver_availability_df
from etl.silver.weather_silver import create_silver_weather_df
from etl.silver.station_silver import create_silver_station_df


if __name__ == "__main__":
    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # création des dataframes silver nécessaires
    df_availability_silver = create_silver_availability_df(spark)[0]
    df_weather_silver = create_silver_weather_df(spark)[0]
    df_station_silver = create_silver_station_df(spark)[0]

    # jointure des dataframes silver pour créer le dataframe gold quotidien
    df_join_availability_station = df_availability_silver.join(
        df_station_silver, "station_id", "inner"
    )

    # # Convertir la colonne "timestamp" en type timestamp (df.printSchema() pour vérifier type)
    # df_join_availability_station = df_join_availability_station.withColumn(
    #     "timestamp", F.col("timestamp").cast("timestamp")
    # )

    # from_unixtime: transforme un timestamp ou string en nombre de secondes depuis 1970-01-01 00:00:00 UTC
    # / 3600 * 3600 arrondi à l'heure la plus proche
    # unix_timestamp: inverse de from_unixtime, transforme un nombre de secondes en timestamp (correpond aussi à 1970)
    # /!\ timestamp_rounded => string
    df_join_availability_station = df_join_availability_station.withColumn(
        "timestamp_rounded",
        F.from_unixtime(F.round(F.unix_timestamp(F.col("timestamp")) / 3600) * 3600),
    )

    # creation du dataframe gold (jointure de tous les df silver)
    df_gold = df_join_availability_station.join(
        df_weather_silver, df_join_availability_station["timestamp_rounded"] == df_weather_silver["timestamp"], "left"
    )

    # df_gold.show(5)
    # df_weather_silver.show(5)
    # df_gold.printSchema()

    # spark-submit etl/gold_scripts/gold_daily.py
