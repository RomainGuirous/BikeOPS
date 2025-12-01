# region IMPORTS
import os
from pyspark.sql import SparkSession, DataFrame
from etl.utils.spark_functions import read_csv_spark, create_silver_df, quality_rapport
from etl.utils.udf import (
    clean_positive_int,
    clean_latitude,
    clean_longitude,
    clean_station_name,
)
# endregion

# region FUNCTIONS


def create_silver_station_df(spark: SparkSession, df_input: DataFrame=None) -> tuple[DataFrame, dict]:
    """
    Crée un DataFrame Spark nettoyé pour les données des stations.

    Args:
        spark (SparkSession): La session Spark active.

    Returns:
        tuple: Un tuple contenant :
        - df_station_clean (DataFrame): DataFrame Spark nettoyé des données des stations:
            - station_id (IntegerType): Identifiant de la station (entier positif).
            - station_name (StringType): Nom de la station (chaîne de caractères propre).
            - lat (FloatType): Latitude de la station.
            - lon (FloatType): Longitude de la station.
            - capacity (IntegerType): Capacité de la station.
        - station_rapport_value (dict): Rapport de qualité des données des stations.
    """
    if df_input is None:
        df = read_csv_spark(spark, "/app/data/data_raw/stations.csv", delimiter=",")
    else:
        df = df_input

    transformations = [
        {
            "col": "station_id",
            "func": clean_positive_int,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "station_name",
            "func": clean_station_name,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "lat",
            "func": clean_latitude,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "lon",
            "func": clean_longitude,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "capacity",
            "func": clean_positive_int,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
    ]

    df_station_clean, station_rapport_value = create_silver_df(
        df, transformations, score=False, duplicates_drop=None, partition_col=None
    )

    return df_station_clean, station_rapport_value


# endregion


# region MAIN SCRIPT


if __name__ == "__main__":
    # ======CREATION DATAFRAME======

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df_clean, rapport_value = create_silver_station_df(spark)

    # ======RAPPORT QUALITE======

    quality_rapport(rapport_value, "stations_silver")

    # ======ECRITURE======

    # creation du répertoire data_clean/silver/stations_silver s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver/stations_silver", exist_ok=True)

    # ======EN PANDAS======

    # utiliser pandas pour créer csv
    pandas_df = df_clean.toPandas()
    pandas_df.to_csv(
        "/app/data/data_clean/silver/stations_silver/stations_silver.csv", index=False
    )

    # ======EN SPARK====== (quel intérêt ?)
    # df_clean.write.mode("overwrite") \
    # .parquet("/app/data/data_clean/silver/stations_silver")

    # arrêt de la session Spark
    spark.stop()

# endregion

# spark-submit etl/silver/station_silver.py
