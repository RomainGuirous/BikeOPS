import os
from pyspark.sql import SparkSession, functions as F

from etl.utils.udf import (
    clean_positive_int,
    clean_date,
    clean_nb_bikes,
)
from etl.utils.spark_functions import (
    read_csv_spark,
    apply_transformations,
    process_report_and_cleanup,
    drop_duplicates
)

if __name__ == "__main__":
    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # lecture du fichier CSV
    df = read_csv_spark(spark, "/app/data/data_raw/availability_raw.csv")

    # nombre de lignes brutes => pour rapport qualité
    lignes_brutes = df.count()

    # extraction colonnes station_id et capacity depuis stations.csv
    capacity = read_csv_spark(
        spark,
        "/app/data/data_raw/stations.csv",
        ["station_id", "capacity"],
        delimiter=",",
    )

    # jointure pour ajouter la capacité
    df = df.join(capacity, on="station_id", how="left")
    df.show(30)
    

    # nom des colonnes accumulateurs
    lignes_corrigees = "lignes_corrigees"
    valeurs_invalides = "valeurs_invalides"

    # nettoyage des données
    # score = nombre de champs invalides (null ou "") dans la ligne
    # df_clean = (
    #     df.withColumn("station_id", clean_positive_int("station_id"))
    #     .withColumn("timestamp", clean_date("timestamp"))
    #     .withColumn(
    #         "bikes_available",
    #         clean_nb_bikes("bikes_available", "slots_free", "capacity"),
    #     )
    #     .withColumn(
    #         "slots_free", clean_nb_bikes("slots_free", "bikes_available", "capacity")
    #     )
    #     .withColumn(
    #         "score",
    #         F.when(
    #             F.col("station_id").isNull() | (F.col("station_id") == ""), 1
    #         ).otherwise(0)
    #         + F.when(
    #             F.col("timestamp").isNull() | (F.col("timestamp") == ""), 1
    #         ).otherwise(0)
    #         + F.when(
    #             F.col("bikes_available").isNull() | (F.col("bikes_available") == ""), 1
    #         ).otherwise(0)
    #         + F.when(
    #             F.col("slots_free").isNull() | (F.col("slots_free") == ""), 1
    #         ).otherwise(0),
    #     )
    #     .withColumn("date_partition", F.to_date(F.col("timestamp")))
    #     .drop("capacity")
    # )
    transformations = [
        {
            "col": "station_id",
            "func": clean_positive_int,
            "args": [lignes_corrigees, valeurs_invalides],
        },
        {
            "col": "timestamp",
            "func": clean_date,
            "args": [lignes_corrigees, valeurs_invalides],
        },
        {
            "col": "bikes_available",
            "func": clean_nb_bikes,
            "args": [lignes_corrigees, valeurs_invalides, "slots_free", "capacity"],
        },
        {
            "col": "slots_free",
            "func": clean_nb_bikes,
            "args": [
                lignes_corrigees,
                valeurs_invalides,
                "bikes_available",
                "capacity",
            ],
        },
    ]

    df_clean = apply_transformations(
        df, transformations, score=True, drop=["capacity"]
    ).withColumn("date_partition", F.to_date(F.col("timestamp")))
    df_clean.show(30)

    # génération du rapport de qualité et nettoyage des colonnes accumulateurs
    df_clean, rapport_value = process_report_and_cleanup(df_clean)
    df_clean.show(30)

    # suppression des doublons station_id,timestamp en gardant la ligne avec le score le plus bas
    df_clean = drop_duplicates(df_clean, partition_cols=["station_id", "timestamp"], order_col="score")

    
    
    # suppression des lignes où toutes les valeurs sont nulles
    df_clean = df_clean.dropna(how="all")
    lignes_supprimees = lignes_brutes - df_clean.count()
    
    # creation du répertoire data_clean/silver s'il n'existe pas
    os.makedirs("/app/data/data_clean/silver", exist_ok=True)

    # ======PANDAS======
    # # conversion en pandas pour sauvegarde en CSV
    # pandas_df = df_dedup.toPandas()

    # # toPandas() transforme les nulls en float, on remet en Int64
    # for c in ("bikes_available", "slots_free"):
    #     if c in pandas_df.columns:
    #         pandas_df[c] = pandas_df[c].astype("Int64")

    # # sauvegarde en CSV
    # pandas_df.to_csv("./data_clean/availability_silver.csv", index=False)

    # ======SPARK======
    df_clean.write.mode("overwrite").partitionBy("date_partition").parquet(
        "/app/data/data_clean/silver/availability_silver"
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
    os.makedirs("/app/data/data_clean/rapport_qualite", exist_ok=True)

    # Écriture du rapport qualité dans un fichier texte
    with open(
        "/app/data/data_clean/rapport_qualite/availability_silver_rapport.txt", "w"
    ) as f:
        f.write("\n".join(rapport))

    # arrêt de la session Spark
    spark.stop()

    # export PYTHONPATH=/app
    # spark-submit etl/silver/availability_silver.py
