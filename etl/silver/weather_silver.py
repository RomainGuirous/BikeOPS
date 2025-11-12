from pyspark.sql import SparkSession, functions as F
from etl.utils.spark_functions import (
    read_csv_spark,
    apply_transformations,
)
from etl.utils.udf import (
    clean_date,
    clean_temperature,
    clean_rain_mm,
    clean_weather,
)
import os





if __name__ == "__main__":
    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    lignes_corrigees = spark.sparkContext.accumulator(0)
    lignes_supprimees = spark.sparkContext.accumulator(0)
    valeurs_invalides = spark.sparkContext.accumulator(0)

    # lecture du fichier CSV
    df = read_csv_spark(spark, "./data/data_raw/weather_raw.csv")

    lignes_brutes = df.count()

    # nettoyage des données
    # df_clean = (
    #     df.withColumn("timestamp", clean_date(F.col("timestamp")))
    #     .withColumn("temperature_c", clean_temperature(F.col("temperature_c")))
    #     .withColumn("rain_mm", clean_rain_mm(F.col("rain_mm")))
    #     .withColumn("weather_condition", clean_weather(F.col("weather_condition")))
    #     .withColumn("date_partition", F.to_date(F.col("timestamp")))
    # )  # pour avoir format a-m-j
    transformations = [
        {"col": "timestamp", "func": clean_date, "args": [valeurs_invalides]},
        {"col": "temperature_c", "func": clean_temperature, "args": [lignes_corrigees, valeurs_invalides]},
        {"col": "rain_mm", "func": clean_rain_mm, "args": [lignes_corrigees, valeurs_invalides]},
        {"col": "weather_condition", "func": clean_weather, "args": [valeurs_invalides]},
        {"col": "date_partition", "func": F.to_date, "args": [F.col("timestamp")]},
    ]

    df_clean = apply_transformations(df, transformations, score=True)

    # suppression des lignes où toutes les valeurs sont nulles
    df_clean = df_clean.dropna(how="all")
    lignes_supprimees += lignes_brutes - df_clean.count()

    # creation du répertoire data_clean s'il n'existe pas
    os.makedirs("../data_clean/", exist_ok=True)

    # # utiliser pandas pour créer csv
    # pandas_df = df_clean.toPandas()
    # pandas_df.to_csv("../data_clean/weather_silver.csv", index=False)

    # utiliser spark pour créer fichier parquet (nécessite environnement et paramètres spécifiques)
    df_clean.write.mode("overwrite").partitionBy("date_partition").parquet(
        "./data_clean/weather_silver"
    )

    # =====RAPPORT QUALITE=====
    rapport = [
        "Rapport qualité - weather_silver",
        "-------------------------------------",
        f"- Lignes brutes : {lignes_brutes}",
        f"- Lignes corrigées : {lignes_corrigees}",
        f"- Lignes invalidées (remplacées par None) : {valeurs_invalides}",
        f"- Lignes supprimées : {lignes_supprimees}",
    ]

    # Création du répertoire rapport_qualite s'il n'existe pas
    os.makedirs("./data_clean/rapport_qualite", exist_ok=True)

    # Écriture du rapport qualité dans un fichier texte
    with open("./data_clean/rapport_qualite/weather_rapport.txt", "w") as f:
        f.write("\n".join(rapport))

    # arrêt de la session Spark
    spark.stop()

    # spark-submit etl/silver/weather_silver.py
