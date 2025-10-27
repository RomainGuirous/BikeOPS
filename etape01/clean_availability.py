from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
import pandas as pd
import os

# le décorateur permet à Spark d'utiliser cette fonction
@F.udf(T.IntegerType())
def clean_positive_int(s: str) -> int | None:
    """
    Nettoie une chaîne de caractères pour obtenir un entier positif.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
        
    Returns:
        int or None: L'entier positif si la conversion est réussie et que l'entier est positif, sinon None.
    """
    global lignes_corrigees, valeurs_invalides
    if not s:
        valeurs_invalides +=1
        return None
    try:
        val = int(s)
        if val > 0:
            if val != s:
                lignes_corrigees +=1
            return val
        else:
            valeurs_invalides +=1
            return None
    # on attend ValueError ou TypeError, on attrape les deux pour éviter de masquer d'autres erreurs
    except (ValueError, TypeError):
        valeurs_invalides +=1
        return None

@F.udf(T.StringType())
def clean_date(s: str) -> str | None:
    """
    Nettoie une chaîne de caractères pour obtenir une date au format YYYY-MM-DD HH:MM:SS.

    Args:
        s (str): La chaîne de caractères à nettoyer.
        
    Returns:
        str or None: La date formatée si la conversion est réussie, sinon None.
    """
    global lignes_corrigees, valeurs_invalides
    if not s:
        valeurs_invalides +=1
        return None
    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isnull(d):
            valeurs_invalides +=1
            return None
        formated_date = d.strftime("%Y-%m-%d %H:%M:%S")
        if formated_date != s:
                lignes_corrigees +=1
        return formated_date
    except (ValueError, TypeError):
        valeurs_invalides +=1
        return None
    
@F.udf(T.IntegerType())
def clean_nb_bikes(a: str, b: str, capacity: str) -> int | None:
    """
    Nettoie le nombre de vélos disponibles ou de places libres en fonction de la capacité de la station.
    
    Args:
        a (str): Le nombre de vélos disponibles ou de places libres à nettoyer.
        b (str): Le nombre complémentaire (places libres si a est vélos disponibles, et vice versa).
        capacity (str): La capacité totale de la station.
        
    Returns:
        int or None: Le nombre nettoyé si la conversion est réussie et que les contraintes sont respectées, sinon None.
    """
    global lignes_corrigees, valeurs_invalides
    if not a or not a.isnumeric():
        if b and capacity:
            try:
                diff = int(capacity) - int(b)
                if diff >= 0:
                    return diff
                else:
                    return 0
            except (ValueError, TypeError):
                valeurs_invalides +=1
                return None
    try:
        val = int(a)
        if val < 0:
            valeurs_invalides +=1
            return 0
        elif val > int(capacity):
            return int(capacity)
        else:
            return val
    except (ValueError, TypeError):
        valeurs_invalides +=1
        return None
    

if __name__ == "__main__":

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    lignes_corrigees = spark.sparkContext.accumulator(0)
    lignes_supprimees = spark.sparkContext.accumulator(0)
    valeurs_invalides = spark.sparkContext.accumulator(0)

    # lecture du fichier CSV
    df = spark.read.option("header", True) \
        .option("sep", ";") \
        .option("mode", "DROPMALFORMED") \
        .csv("./data_raw/availability_raw.csv")
    
    # extraction colonnes station_id et capacity depuis stations.csv
    capacity = spark.read.option("header", True) \
        .option("sep", ",") \
        .option("mode", "DROPMALFORMED") \
        .csv("./data_raw/stations.csv") \
        .select("station_id", "capacity")
    
    # jointure pour ajouter la capacité
    df = df.join(capacity, on="station_id", how="left")
    
    # nettoyage des données
    # score = nombre de champs invalides (null ou "") dans la ligne
    df_clean = df.withColumn("station_id", clean_positive_int("station_id")) \
        .withColumn("timestamp", clean_date("timestamp")) \
        .withColumn("bikes_available", clean_nb_bikes("bikes_available", "slots_free", "capacity")) \
        .withColumn("slots_free", clean_nb_bikes("slots_free", "bikes_available", "capacity")) \
        .withColumn(
            "score",
            F.when(F.col("station_id").isNull() | (F.col("station_id") == ""), 1).otherwise(0) +
            F.when(F.col("timestamp").isNull() | (F.col("timestamp") == ""), 1).otherwise(0) +
            F.when(F.col("bikes_available").isNull() | (F.col("bikes_available") == ""), 1).otherwise(0) +
            F.when(F.col("slots_free").isNull() | (F.col("slots_free") == ""), 1).otherwise(0)
        ) \
        .withColumn("date_partition", F.to_date(F.col("timestamp"))) \
        .drop("capacity")
    # score = nombre de champs invalides (null ou "") dans la ligne

    # group by station_id et timestamp, order by score
    dup_wind = Window.partitionBy("station_id", "timestamp").orderBy("score")

    # garder la ligne avec le score le plus bas (la meilleure)
    df_dedup = df_clean.withColumn("rn", F.row_number().over(dup_wind)) \
                       .filter(F.col("rn") == 1) \
                       .drop("score", "rn")

    # creation du répertoire data_clean s'il n'existe pas
    os.makedirs("../data_clean/", exist_ok=True)

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
    df_clean.write.mode("overwrite") \
    .partitionBy("date_partition") \
    .parquet("./data_clean/availability_silver")

    # =====RAPPORT QUALITE=====
    rapport = [
    "Rapport qualité - weather_silver",
    "-------------------------------------",
    f"- Lignes brutes : {lignes_brutes}",
    f"- Lignes corrigées : {lignes_corrigees}",
    f"- Lignes invalidées (remplacées par None) : {valeurs_invalides}",
    f"- Lignes supprimées : {lignes_supprimees}"
    ]

    # Création du répertoire rapport_qualite s'il n'existe pas
    os.makedirs("./data_clean/rapport_qualite", exist_ok=True)

    # Écriture du rapport qualité dans un fichier texte
    with open("./data_clean/rapport_qualite/weather_rapport.txt", "w") as f:
        f.write("\n".join(rapport))

    # arrêt de la session Spark
    spark.stop()

    # spark-submit etape01/clean_availability.py
