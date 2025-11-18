import os
from pyspark.sql import SparkSession
from etl.utils.spark_functions import read_csv_spark, create_silver_df, quality_rapport
from etl.utils.udf import (
    clean_positive_int,
    clean_latitude,
    clean_longitude,
    clean_station_name,
)


if __name__ == "__main__":
    # ======CREATION DATAFRAME======

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # def create_silver_availability_df():
    #     # lecture du fichier CSV
    #     df = read_csv_spark(spark, "/app/data/data_raw/stations.csv", delimiter=",")

    #     # nettoyage des données
    #     df_clean = (
    #         df.withColumn("station_id", clean_positive_int(F.col("station_id")))
    #         .withColumn("station_name", clean_station_name(F.col("station_name")))
    #         .withColumn("lat", clean_latitude(F.col("lat")))
    #         .withColumn("lon", clean_longitude(F.col("lon")))
    #         .withColumn(
    #             "capacity", clean_positive_int(F.col("capacity"))
    #         )
    #     )

    #     return df_clean

    # lecture du fichier CSV
    df = read_csv_spark(spark, "/app/data/data_raw/stations.csv", delimiter=",")

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

    df_clean, rapport_value = create_silver_df(
        df, transformations, score=False, duplicates_drop=False, partition_col=None
    )

    # ======RAPPORT QUALITE======

    quality_rapport(rapport_value, "stations_silver")

    # ======ECRITURE======

    # creation du répertoire data_clean/silver/stations_silver s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver/stations_silver", exist_ok=True)

    # ======EN PANDAS======

    # utiliser pandas pour créer csv
    pandas_df = df_clean.toPandas()
    pandas_df.to_csv("/app/data/data_clean/silver/stations_silver/stations_silver.csv", index=False)

    # ======EN SPARK====== (quel intérêt ?)
    # df_clean.write.mode("overwrite") \
    # .parquet("/app/data/data_clean/silver/stations_silver")

    # arrêt de la session Spark
    spark.stop()

    # spark-submit etl/silver/station_silver.py
