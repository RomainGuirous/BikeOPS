# region IMPORTS
import os
from pyspark.sql import SparkSession, DataFrame, functions as F

from etl.utils.udf import (
    clean_positive_int,
    clean_date,
    clean_nb_bikes,
)
from etl.utils.spark_functions import (
    read_csv_spark,
    create_silver_df,
    quality_rapport,
)
# endregion

# region FUNCTIONS


def create_silver_availability_df(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    Crée un DataFrame Spark nettoyé pour les données de disponibilité des vélos.

    Args:
        spark (SparkSession): La session Spark active.

    Returns:
        tuple: Un tuple contenant :
        - df_availability_clean (DataFrame): DataFrame Spark nettoyé des données de disponibilité.
        - availability_rapport_value (dict): Rapport de qualité des données de disponibilité.
    """
    # lecture du fichier CSV
    df = read_csv_spark(spark, "/app/data/data_raw/availability_raw.csv")

    # extraction colonnes station_id et capacity depuis stations.csv
    capacity = read_csv_spark(
        spark,
        "/app/data/data_raw/stations.csv",
        ["station_id", "capacity"],
        delimiter=",",
    )

    # jointure pour ajouter la capacité
    df = df.join(capacity, on="station_id", how="left")

    # définition des  transformations à appliquer
    transformations = [
        {
            "col": "station_id",
            "func": clean_positive_int,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "timestamp",
            "func": clean_date,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "bikes_available",
            "func": clean_nb_bikes,
            "args": [
                "slots_free",
                "capacity",
                "lignes_corrigees",
                "valeurs_invalides",
            ],
        },
        {
            "col": "slots_free",
            "func": clean_nb_bikes,
            "args": [
                "bikes_available",
                "capacity",
                "lignes_corrigees",
                "valeurs_invalides",
            ],
        },
    ]

    # création du DataFrame silver avec nettoyage et rapport qualité
    df_availability_clean, availability_rapport_value = create_silver_df(
        df,
        transformations,
        score=True,
        duplicates_drop=["station_id", "timestamp"],
        partition_col="timestamp",
        drop_cols=["capacity", "score"],
    )

    return df_availability_clean, availability_rapport_value


# endregion

# region MAIN SCRIPT
if __name__ == "__main__":
    # ======CREATION DATAFRAME======

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df_clean, rapport_value = create_silver_availability_df(spark)

    # Filtrer les lignes où "bikes_available" ou "slots_free" sont null ou None
    df_partial_null_values = df_clean.filter(
        (
            (F.col("slots_free").isNull() & F.col("bikes_available").isNull())
            & F.col("capacity").isNotNull()
        )
    )
    df_slots_null_values = df_clean.filter(F.col("slots_free").isNull())
    df_bikes_available_values = df_clean.filter(F.col("bikes_available").isNull())
    df_capacity_values = df_clean.filter(F.col("capacity").isNull())

    # =====RAPPORT QUALITE=====

    quality_rapport(rapport_value, "availability_silver")

    # ======ECRITURE======
    # creation du répertoire data_clean/silver/availability_silver s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver/availability_silver", exist_ok=True)

    # ======EN PANDAS======
    # # conversion en pandas pour sauvegarde en CSV
    # pandas_df = df_dedup.toPandas()

    # # toPandas() transforme les nulls en float, on remet en Int64
    # for c in ("bikes_available", "slots_free"):
    #     if c in pandas_df.columns:
    #         pandas_df[c] = pandas_df[c].astype("Int64")

    # # sauvegarde en CSV
    # pandas_df.to_csv("./data_clean/availability_silver.csv", index=False)

    # ======EN SPARK======
    df_clean.write.mode("overwrite").partitionBy("date_partition").parquet(
        "/app/data/data_clean/silver/availability_silver"
    )

    # arrêt de la session Spark
    spark.stop()

# endregion

# spark-submit etl/silver/availability_silver.py
