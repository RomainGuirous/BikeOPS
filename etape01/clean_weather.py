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

@F.udf(T.DoubleType())
def clean_temperature(s: str) -> float | None:
    """
    Nettoie une chaîne de caractères pour obtenir une température en degrés Celsius.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
    
    Returns:
        float or None: La température en degrés Celsius si la conversion est réussie et que la température est dans un intervalle réaliste, sinon None.
    """
    global lignes_corrigees, valeurs_invalides
    if not s:
        valeurs_invalides +=1
        return None
    try:
        formated_line = str(s).replace(",", ".")
        val = float(formated_line)
        if (val >= -20 and val <= 50):
            if formated_line != s:
                lignes_corrigees +=1
            return val
        else:
            valeurs_invalides +=1
            return None
    except (ValueError, TypeError):
        valeurs_invalides +=1
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
    global lignes_corrigees, valeurs_invalides
    if not s:
        return None
    try:
        formated_line = str(s).replace(",", ".")
        val = float(formated_line)
        if val >= 0:
            if formated_line != s:
                lignes_corrigees +=1
            return val
        else:
            valeurs_invalides +=1
            return None
    except (ValueError, TypeError):
        valeurs_invalides +=1
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
    global lignes_corrigees, valeurs_invalides
    if not s:
        valeurs_invalides +=1
        return None
    try:
        if s in ['Rain', 'Cloudy', 'Clear', 'Drizzle', 'Fog']:
            return s
        else:
            valeurs_invalides +=1
            return None
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
        .csv("./data_raw/weather_raw.csv")
    
    lignes_brutes = df.count()

    # nettoyage des données
    df_clean = df.withColumn("timestamp", clean_date(F.col("timestamp"))) \
      .withColumn("temperature_c", clean_temperature(F.col("temperature_c"))) \
      .withColumn("rain_mm", clean_rain_mm(F.col("rain_mm"))) \
      .withColumn("weather_condition", clean_weather(F.col("weather_condition"))) \
      .withColumn("date_partition", F.to_date(F.col("timestamp"))) # pour avoir format a-m-j

    # suppression des lignes où toutes les valeurs sont nulles
    df_clean = df_clean.dropna(how="all")
    lignes_supprimees += lignes_brutes - df_clean.count()

    # creation du répertoire data_clean s'il n'existe pas
    os.makedirs("../data_clean/", exist_ok=True)

    # # utiliser pandas pour créer csv
    # pandas_df = df_clean.toPandas()
    # pandas_df.to_csv("../data_clean/weather_silver.csv", index=False)

    # utiliser spark pour créer fichier parquet (nécessite environnement et paramètres spécifiques)
    df_clean.write.mode("overwrite") \
    .partitionBy("date_partition") \
    .parquet("./data_clean/weather_silver")

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

    # spark-submit etape01/clean_weather.py
