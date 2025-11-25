# region IMPORTS
import os
from pyspark.sql import SparkSession, DataFrame
from etl.utils.spark_functions import (
    read_csv_spark,
    create_silver_df,
    quality_rapport,
)
from etl.utils.udf import (
    clean_date,
    clean_temperature,
    clean_rain_mm,
    clean_weather,
)
# endregion

# region FUNCTIONS


def create_silver_weather_df(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    Crée un DataFrame Spark nettoyé pour les données météo.

    Args:
        spark (SparkSession): La session Spark active.

    Returns:
        tuple: Un tuple contenant :
        - df_weather_clean (DataFrame): DataFrame Spark nettoyé des données météo.
        - weather_rapport_value (dict): Rapport de qualité des données météo.
    """
    # lecture du fichier CSV
    df = read_csv_spark(spark, "/app/data/data_raw/weather_raw.csv")

    # liste des fonctions de nettoyage à appliquer
    transformations = [
        {
            "col": "timestamp",
            "func": clean_date,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "temperature_c",
            "func": clean_temperature,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "rain_mm",
            "func": clean_rain_mm,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
        {
            "col": "weather_condition",
            "func": clean_weather,
            "args": ["lignes_corrigees", "valeurs_invalides"],
        },
    ]

    df_weather_clean, weather_rapport_value = create_silver_df(
        df,
        transformations,
        score=False,
        duplicates_drop=False,
        partition_col="timestamp",
    )

    return df_weather_clean, weather_rapport_value


# endregion


# region MAIN SCRIPT


if __name__ == "__main__":
    # ======CREATION DATAFRAME======

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df_clean, rapport_value = create_silver_weather_df(spark)

    # =====RAPPORT QUALITE=====

    quality_rapport(rapport_value, "weather_silver")

    # ======ECRITURE======
    # creation du répertoire data_clean/silver/weather_silver s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver/weather_silver", exist_ok=True)

    # ======EN PANDAS======
    # # conversion en pandas pour sauvegarde en CSV
    # pandas_df = df_clean.toPandas()
    # pandas_df.to_csv("/app/data/data_clean/silver/weather_silver/weather_silver.csv", index=False)

    # ======EN SPARK======
    # utiliser spark pour créer fichier parquet (nécessite environnement et paramètres spécifiques)
    df_clean.write.mode("overwrite").partitionBy("date_partition").parquet(
        "/app/data/data_clean/silver/weather_silver"
    )

    # arrêt de la session Spark
    spark.stop()

# endregion

# spark-submit etl/silver/weather_silver.py
