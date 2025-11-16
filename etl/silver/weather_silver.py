import os
from pyspark.sql import SparkSession, functions as F
from etl.utils.spark_functions import (
    read_csv_spark,
    apply_transformations,
    process_report_and_cleanup,
)
from etl.utils.udf import (
    clean_date,
    clean_temperature,
    clean_rain_mm,
    clean_weather,
)


if __name__ == "__main__":
    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # lecture du fichier CSV
    df = read_csv_spark(spark, "/app/data/data_raw/weather_raw.csv")

    # nombre de lignes brutes => pour rapport qualité
    lignes_brutes = df.count()
    
    # nom des colonnes accumulateurs
    lignes_corrigees = "lignes_corrigees"
    valeurs_invalides = "valeurs_invalides"
    
    transformations = [
        {
            "col": "timestamp",
            "func": clean_date,
            "args": [lignes_corrigees, valeurs_invalides],
        },
        {
            "col": "temperature_c",
            "func": clean_temperature,
            "args": [lignes_corrigees, valeurs_invalides],
        },
        {
            "col": "rain_mm",
            "func": clean_rain_mm,
            "args": [lignes_corrigees, valeurs_invalides],
        },
        {
            "col": "weather_condition",
            "func": clean_weather,
            "args": [lignes_corrigees,valeurs_invalides],
        }
    ]

    #suite au transformations udf, timestamp est un struct avec value, ligne_corrigee, ligne_invalide
    df_clean = apply_transformations(
        df, transformations, score=True
        ).withColumn("date_partition", F.to_date(F.col("timestamp")))


    # génération du rapport de qualité et nettoyage des colonnes accumulateurs
    df_clean, rapport_value = process_report_and_cleanup(df_clean)
    
    # suppression des lignes où toutes les valeurs sont nulles
    df_clean = df_clean.dropna(how="all")
    lignes_supprimees = lignes_brutes - df_clean.count()

    # creation du répertoire data_clean/silver s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver", exist_ok=True)

    # # utiliser pandas pour créer csv
    # pandas_df = df_clean.toPandas()
    # pandas_df.to_csv("/app/data/data_clean/silver/weather_silver.csv", index=False)

    # utiliser spark pour créer fichier parquet (nécessite environnement et paramètres spécifiques)
    df_clean.write.mode("overwrite").partitionBy("date_partition").parquet(
        "/app/data/data_clean/silver/weather_silver"
    )

    # =====RAPPORT QUALITE=====
    rapport = [
        "Rapport qualité - weather_silver",
        "-------------------------------------",
        f"- Lignes brutes : {lignes_brutes}",
        f"- Lignes corrigées : {rapport_value['total_lignes_corrigees']}",
        f"- Lignes invalidées (remplacées par None) : {rapport_value['total_valeurs_invalides']}",
        f"- Lignes supprimées : {lignes_supprimees}",
    ]

    # Création du répertoire rapport_qualite s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver/rapport_qualite", exist_ok=True)

    # Écriture du rapport qualité dans un fichier texte
    with open("/app/data/data_clean/rapport_qualite/weather_silver_rapport.txt", "w") as f:
        f.write("\n".join(rapport))

    # arrêt de la session Spark
    spark.stop()

    # export PYTHONPATH=/app
    # spark-submit etl/silver/weather_silver.py
