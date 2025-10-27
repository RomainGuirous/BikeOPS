from pyspark.sql import SparkSession, functions as F, types as T
import pandas as pd
import os
import re

@F.udf(T.IntegerType())
def clean_positive_int(s: str) -> int | None:
    """
    Nettoie une chaîne de caractères pour obtenir un entier positif.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
        
    Returns:
        int or None: L'entier positif si la conversion est réussie et que l'entier est positif, sinon None.
    """
    if not s:
        return None
    try:
        val = int(s)
        return val if val > 0 else None
    # on attend ValueError ou TypeError, on attrape les deux pour éviter de masquer d'autres erreurs
    except (ValueError, TypeError):
        return None
    
@F.udf(T.FloatType())
def clean_latitude(s: str) -> float | None:
    if not s:
        return None
    try:
        val = float(s)
        return val if val <= 90 and val >= -90 else None
    except (ValueError, TypeError):
        return None
    
@F.udf(T.FloatType())
def clean_longitude(s: str) -> float | None:
    if not s:
        return None
    try:
        val = float(s)
        return val if val <= 180 and val >= -180 else None
    except (ValueError, TypeError):
        return None

@F.udf(T.StringType())
def clean_station_name(s: str) -> str | None:
    if not s:
        return None
    try:
        pattern =  r'^Lille - Station \d{2}$'
        if re.fullmatch(pattern, s):
            return s
        else:
            return None

    except (ValueError, TypeError):
        return None


if __name__ == "__main__":

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # lecture du fichier CSV
    df = spark.read.option("header", True) \
        .option("sep", ",") \
        .option("mode", "DROPMALFORMED") \
        .csv("./data_raw/stations.csv")
    
    # nettoyage des données
    df_clean = (
    df.withColumn("station_id", clean_positive_int(F.col("station_id")))
      .withColumn("station_name", clean_station_name(F.col("station_name")))
      .withColumn("lat", clean_latitude(F.col("lat")))
      .withColumn("lon", clean_longitude(F.col("lon")))
      .withColumn("capacity", clean_positive_int(F.col("capacity"))) # pour avoir format a-m-j
    )

    # creation du répertoire data_clean s'il n'existe pas
    os.makedirs("../data_clean/", exist_ok=True)

    # utiliser pandas pour créer csv
    pandas_df = df_clean.toPandas()
    pandas_df.to_csv("./data_clean/stations_silver.csv", index=False)


    # df_clean.write.mode("overwrite") \
    # .parquet("./data_clean/stations_silver")

    # arrêt de la session Spark
    spark.stop()

    # spark-submit etape01/clean_station.py