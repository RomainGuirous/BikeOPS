from pyspark.sql import SparkSession, functions as F, types as T
import pandas as pd
import os

@F.udf(T.StringType())
def clean_date(s: str) -> str | None:
    """
    Nettoie une chaîne de caractères pour obtenir une date au format YYYY-MM-DD HH:MM:SS.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
    
    Returns:
        str or None: La date formatée si la conversion est réussie, sinon None.
    """
    if not s:
        return None
    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isnull(d):
            return None
        return d.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None

@F.udf(T.DoubleType())
def clean_temperature(s: str) -> float | None:
    """
    Nettoie une chaîne de caractères pour obtenir une température en degrés Celsius.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
    
    Returns:
        float or None: La température en degrés Celsius si la conversion est réussie et que la température est dans un intervalle réaliste, sinon None.
    """
    if not s:
        return None
    s = str(s).replace(",", ".")
    try:
        val = float(s)
        return val if (val >= -20 and val <= 50) else None
    except (ValueError, TypeError):
        return None
    
@F.udf(T.DoubleType())
def clean_rain_mm(s: str) -> float | None:
    """
    Nettoie une chaîne de caractères pour obtenir une quantité de pluie en millimètres.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
    
    Returns:
        float or None: La quantité de pluie en millimètres si la conversion est réussie et que la valeur est positive, sinon None.
    """
    if not s:
        return None
    s = str(s).replace(",", ".")
    try:
        val = float(s)
        return val if val >= 0 else None
    except (ValueError, TypeError):
        return None
    
@F.udf(T.StringType())
def clean_weather(s: str) -> str | None:
    """
    Nettoie une chaîne de caractères pour obtenir une condition météorologique.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
    
    Returns:
        str or None: La condition météorologique si la valeur est valide, sinon None.
    """
    if not s:
        return None
    try:
        return s if s in ['Rain', 'Cloudy', 'Clear', 'Drizzle', 'Fog'] else None
    except (ValueError, TypeError):
        return None
    

if __name__ == "__main__":

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # lecture du fichier CSV
    df = spark.read.option("header", True) \
        .option("sep", ";") \
        .option("mode", "DROPMALFORMED") \
        .csv("../data_raw/weather_raw.csv")
    
    # nettoyage des données
    df_clean = (
    df.withColumn("timestamp", clean_date(F.col("timestamp")))
      .withColumn("temperature_c", clean_temperature(F.col("temperature_c")))
      .withColumn("rain_mm", clean_rain_mm(F.col("rain_mm")))
      .withColumn("weather_condition", clean_weather(F.col("weather_condition")))
      .withColumn("date_partition", F.to_date(F.col("timestamp")))
    )

    # creation du répertoire data_clean s'il n'existe pas
    os.makedirs("../data_clean/", exist_ok=True)

    # utiliser pandas pour créer csv
    pandas_df = df_clean.toPandas()
    pandas_df.to_csv("../data_clean/clean_weather.csv", index=False)

    # # utiliser spark pour créer fichier parquet (nécessite environnement et paramètres spécifiques)
    # df_clean.write.mode("overwrite") \
    # .partitionBy("date_partition") \
    # .parquet("./data_clean/")

    # arrêt de la session Spark
    spark.stop()

    # spark-submit clean_weather.py
