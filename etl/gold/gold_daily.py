# region IMPORTS
from pyspark.sql import SparkSession, functions as F, DataFrame, types as T
from etl.silver.availability_silver import create_silver_availability_df
from etl.silver.weather_silver import create_silver_weather_df
from etl.silver.station_silver import create_silver_station_df
from pyspark.sql.window import Window
import pandas as pd
# endregion


# ===== CREATION TABLE DIMENSION ======
# * les dimensions doivent êtres descriptives et stables
# * les dimensions ne doivent pas contenir des mesures fluctuantes
# * /!\ les dimensions doivent être utilisées pour filtrer ou agréger les faits /!\

# region DIM STATION
# ------------


def dim_station(df_station_silver: DataFrame) -> DataFrame:
    """
    Création de la table dimensionnelle des stations
    Colonnes:
    - station_id (int)
    - station_name (string)
    - lat (float)
    - lon (float)
    - capacity (int)
    - city (string)
    - station_number (string)
    - station_key (long)

    Args:
        df_station_silver (DataFrame): dataframe silver des stations

    Returns:
        dim_station (DataFrame): table dimensionnelle des stations
    """
    dim_station = (
        df_station_silver.select("station_id", "station_name", "lat", "lon", "capacity")
        .withColumn(
            "city",
            F.regexp_extract(
                F.col("station_name"),
                r"^([A-Za-z\-]+) - Station",  # autoriser noms composés avec tiret
                1,  # 1: groupe 1 (0 est le texte complet)
            ).cast("string"),
        )
        .withColumn(
            "station_number",
            F.regexp_extract(
                F.col("station_name"),
                r"Station (\d+)",
                1,  # 1: groupe 1 (0 est le texte complet)
            ).cast("string"),
        )
        .dropDuplicates()
        # monotonically_increasing_id: façon optimisée de créer id dans système distribué
        # si id autoincrementé, toutes les partitions doivent être scannées pour connaître prochain id à chaque ajout
        .withColumn("station_key", F.monotonically_increasing_id())
    )
    return dim_station


# endregion

# region DIM WEATHER
# -----------


def dim_weather(df_weather_silver: DataFrame) -> DataFrame:
    """
    Création de la table dimensionnelle des conditions météorologiques
    Colonnes:
    - weather_condition (string)
    - weather_condition_key (long)

    Args:
        df_weather_silver (DataFrame): dataframe silver des conditions météorologiques

    Returns:
        dim_weather (DataFrame): table dimensionnelle des conditions météorologiques
    """
    dim_weather = (
        df_weather_silver.select("weather_condition")
        .dropDuplicates()
        .withColumn("weather_condition_key", F.monotonically_increasing_id())
        .orderBy("weather_condition_key")
    )
    return dim_weather


# region DIM DATE
# --------


def dim_date(
    spark: SparkSession, start_date: str = "2025-01-01", end_date: str = "2025-12-31"
):
    """
    Création de la table dimensionnelle de la date
    Colonnes:
    - date (date)
    - year (int)
    - month (int)
    - day (int)
    - quarter (int)
    - day_of_week (string)
    - day_of_week_num (int)
    - is_weekend (boolean)
    - week_of_year (int)
    - iso_year (int)
    - date_key (long)
    
    Args:
        spark (SparkSession): session Spark
        start_date (str): date de début au format 'YYYY-MM-DD'
        end_date (str): date de fin au format 'YYYY-MM-DD'
        
    Returns:
        dim_date (DataFrame): table dimensionnelle de la date
    """
    # Création d'un DataFrame Pandas
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame(
        {
            "date": dates,
            "year": dates.year,
            "month": dates.month,
            "day": dates.day,
            "quarter": dates.quarter,
            "day_of_week": dates.day_name().str[:3],  # abrégé (Mon, Tue, ...)
            "day_of_week_num": dates.weekday + 1,  # lundi=1,...,dimanche=7
            "is_weekend": dates.weekday >= 5,
            "week_of_year": dates.isocalendar().week,  # semaine ISO 8601
            "iso_year": dates.isocalendar().year,  # année ISO pour gérer les semaines chevauchantes
        }
    )

    # Conversion en Spark DataFrame
    sdf = spark.createDataFrame(df)

    # Si tu veux, tu peux caster les colonnes correctement
    sdf = (
        sdf.withColumn("date", F.col("date").cast(T.DateType()))
        .withColumn("year", F.col("year").cast(T.IntegerType()))
        .withColumn("month", F.col("month").cast(T.IntegerType()))
        .withColumn("day", F.col("day").cast(T.IntegerType()))
        .withColumn("quarter", F.col("quarter").cast(T.IntegerType()))
        .withColumn("day_of_week_num", F.col("day_of_week_num").cast(T.IntegerType()))
        .withColumn("is_weekend", F.col("is_weekend").cast(T.BooleanType()))
        .withColumn("week_of_year", F.col("week_of_year").cast(T.IntegerType()))
        .withColumn("iso_year", F.col("iso_year").cast(T.IntegerType()))
        .withColumn("date_key", F.monotonically_increasing_id())
    )

    return sdf


# endregion

# region DIM TIME
# --------


def dim_time(spark: SparkSession) -> DataFrame:
    """
    Création de la table dimensionnelle du temps
    Colonnes:
    - hour (int)
    - minute (int)
    - time_key (int)

    Args:
        spark (SparkSession): session Spark

    Returns:
        dim_time (DataFrame): table dimensionnelle du temps
    """
    dim_time = (
        spark.range(24 * 60)  # Nombre total de minutes dans une journée
        .withColumn("hour", (F.col("id") / 60).cast("int"))
        .withColumn("minute", (F.col("id") % 60).cast("int"))
        .withColumn("time_key", F.col("id"))  # id unique qui sert de clé surrogate
        .drop("id")
        .orderBy("hour", "minute")
    )
    return dim_time


# endregion


# ===== CREATION TABLE FAITS ======
# * les faits doivent contenir des mesures quantitatives
# * les faits doivent contenir des clés étrangères vers les dimensions
# * les faits doivent répondre à des questions métier précises


# region AVG BIKES
# ----------------------------------------


def fact_avg_bikes_available_per_day_and_station(
    df_availability_silver: DataFrame,
    dim_date_df: DataFrame,
    dim_station_df: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits de la moyenne de vélos disponibles par jour et par station
    Colonnes:
    - avg_bikes_available (float)
    - date_key (int) : clé surrogate de la date
    - station_key (int) : clé surrogate de la station
    Args:
        df_availability_silver (DataFrame): dataframe silver de la disponibilité des vélos
        dim_date_df (DataFrame): dataframe dimension date
        dim_station_df (DataFrame): dataframe dimension station

    Returns:
        fact_avg_velo_dispo_per_day_and_station (DataFrame): table de faits de la moyenne de vélos disponibles par jour et par station
    """

    fact_avg_velo_dispo_per_day_and_station = (
        df_availability_silver.groupBy("availability_date_partition", "station_id").agg(
            F.round(F.avg("bikes_available"), 2).alias("avg_bikes_available")
        )
    ).select("availability_date_partition", "station_id", "avg_bikes_available")

    # Jointure avec dim_date pour obtenir la clé surrogate date_key
    fact_avg_velo_dispo_per_day_and_station = (
        fact_avg_velo_dispo_per_day_and_station.join(
            dim_date_df.select("date", "date_key"),
            fact_avg_velo_dispo_per_day_and_station["availability_date_partition"]
            == dim_date_df["date"],
            "left",
        ).drop("availability_date_partition", "date")
    )

    # Jointure avec dim_station pour obtenir la clé surrogate station_key
    fact_avg_velo_dispo_per_day_and_station = (
        fact_avg_velo_dispo_per_day_and_station.join(
            dim_station_df.select("station_id", "station_key"),
            fact_avg_velo_dispo_per_day_and_station["station_id"]
            == dim_station_df["station_id"],
            "left",
        ).drop("station_id")
    )

    return fact_avg_velo_dispo_per_day_and_station


# endregion

# region TAUX OCCUPATION
# ---------------


def fact_taux_occupation(
    df_availability_silver: DataFrame,
    dim_date_df: DataFrame,
    dim_station_df: DataFrame,
    dim_time_df: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits du taux d'occupation des vélos
    Colonnes:
    - taux_occupation (float)
    - date_key (int) : clé surrogate de la date
    - time_key (int) : clé surrogate du temps (heure, minute)
    - station_key (int) : clé surrogate de la station

    Args:
        df_availability_silver (DataFrame): dataframe silver de la disponibilité des vélos
        dim_date_df (DataFrame): dataframe dimension date
        dim_station_df (DataFrame): dataframe dimension station
        dim_time_df (DataFrame): dataframe dimension temps

    Returns:
        fact_taux_occupation (DataFrame): table de faits du taux d'occupation des vélos par station et par tranche horaire
    """
    fact_taux_occupation = df_availability_silver.withColumn(
        "taux_occupation",
        F.round(
            F.col("bikes_available")
            / (F.col("bikes_available") + F.col("slots_free"))
            * 100,
            2,
        ),
    ).select(
        "station_id",
        "availability_date_partition",
        "availability_timestamp",
        "taux_occupation",
    )

    # Jointure avec dim_date pour récupérer la clé surrogate date_key
    fact_taux_occupation = fact_taux_occupation.join(
        dim_date_df.select("date", "date_key"),
        fact_taux_occupation["availability_date_partition"] == dim_date_df["date"],
        "left",
    ).drop("availability_date_partition", "date")

    # Jointure avec dim_time pour récupérer la clé surrogate time_key
    fact_taux_occupation = fact_taux_occupation.withColumn(
        "hour", F.hour("availability_timestamp")
    ).withColumn("minute", F.minute("availability_timestamp"))
    fact_taux_occupation = fact_taux_occupation.join(
        dim_time_df, on=["hour", "minute"], how="left"
    ).drop("hour", "minute", "availability_timestamp")

    # Jointure avec dim_station pour récupérer la clé surrogate station_key
    fact_taux_occupation = fact_taux_occupation.join(
        dim_station_df.select("station_id", "station_key"),
        fact_taux_occupation["station_id"] == dim_station_df["station_id"],
        "left",
    ).drop("station_id", "bikes_available", "slots_free")

    return fact_taux_occupation


# endregion

# region METEO DOMINANTE
# ----------------------


def fact_meteo_dominante(
    df_weather_silver: DataFrame, dim_date_df: DataFrame, dim_weather_df: DataFrame
) -> DataFrame:
    """
    Création de la table de faits de la météo dominante par jour
    Colonnes:
    - count (int): nombre d'occurrences de la condition météo par jour
    - date_key (int): clé surrogate de la date
    - weather_condition_key (long): clé surrogate de la condition météo

    Args:
        df_weather_silver (DataFrame): dataframe silver des conditions météorologiques
        dim_date_df (DataFrame): dataframe dimension date
        dim_weather_df (DataFrame): dataframe dimension météo

    Returns:
        fact_meteo_dominante (DataFrame): table de faits de la météo dominante par jour
    """
    weather_counts = (
        df_weather_silver.groupBy("weather_date_partition", "weather_condition")
        .count()
        .select("weather_date_partition", "weather_condition", "count")
    )

    # On crée une fenêtre par jour, triée par count décroissant
    w = Window.partitionBy("weather_date_partition").orderBy(F.desc("count"))

    # On garde uniquement la condition météo la plus fréquente du jour
    fact_meteo_dominante = (
        weather_counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Jointure avec dim_date pour récupérer la clé surrogate date_key
    fact_meteo_dominante = fact_meteo_dominante.join(
        dim_date_df.select("date", "date_key"),
        fact_meteo_dominante["weather_date_partition"] == dim_date_df["date"],
        "left",
    ).drop("weather_date_partition", "date")

    fact_meteo_dominante = fact_meteo_dominante.join(
        dim_weather_df,
        fact_meteo_dominante["weather_condition"]
        == dim_weather_df["weather_condition"],
        "left",
    ).drop("weather_condition")  # garder weather_condition_key uniquement

    return fact_meteo_dominante


# region TOP 5 STATIONS SATUREES
# -----------------------

# les  % de stations saturées par jour


def fact_station_saturee_per_day(
    df_join_availability_station: DataFrame,
    dim_date_df: DataFrame,
    dim_station_df: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits du pourcentage de stations saturées par jour
    Colonnes:
    - station_saturee (float)
    - date_key (int) : clé surrogate de la date
    - station_key (int) : clé surrogate de la station

    Args:
        df_join_availability_station (DataFrame): dataframe résultant de la jointure entre la disponibilité des vélos et les informations des stations
        dim_date_df (DataFrame): dataframe dimension date
        dim_station_df (DataFrame): dataframe dimension station

    Returns:
        fact_station_saturee_per_day (DataFrame): table de faits du pourcentage de stations saturées par jour
    """
    fact_station_saturee_per_day = (
        df_join_availability_station.groupBy(
            "availability_date_partition", "station_id"
        )
        .agg(
            F.count("*").alias("count_releve_per_day"),
            F.sum(
                F.when(F.col("bikes_available") == F.col("capacity"), 1).otherwise(0)
            ).alias("nbr_station_sature"),
        )
        .withColumn(
            "station_saturee",
            F.round(
                F.col("nbr_station_sature") / F.col("count_releve_per_day") * 100, 2
            ),
        )
        .select("availability_date_partition", "station_id", "station_saturee")
    )

    # Jointure avec dim_date pour récupérer la clé surrogate date_key
    fact_station_saturee_per_day = fact_station_saturee_per_day.join(
        dim_date_df.select("date", "date_key"),
        fact_station_saturee_per_day["availability_date_partition"]
        == dim_date_df["date"],
        "left",
    ).drop("availability_date_partition", "date")

    # Jointure avec dim_station pour récupérer la clé surrogate station_key
    fact_station_saturee_per_day = fact_station_saturee_per_day.join(
        dim_station_df.select("station_id", "station_key"),
        fact_station_saturee_per_day["station_id"] == dim_station_df["station_id"],
        "left",
    ).drop("station_id", "bikes_available", "slots_free")

    return fact_station_saturee_per_day


# les 5 stations les plus saturées par jour (5 lignes par jour)


def fact_top_5_station_saturee_per_day(
    df_join_availability_station: DataFrame,
    dim_date_df: DataFrame,
    dim_station_df: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits du top 5 des stations les plus saturées par jour
    Colonnes:
    - station_saturee (float)
    - date_key (int) : clé surrogate de la date
    - station_key (int) : clé surrogate de la station

    Args:
        df_join_availability_station (DataFrame): dataframe de jointure (pour utiliser fact_station_saturee_per_day())

    Returns:
        fact_top_5_station_saturee_per_day (DataFrame): table de faits du top 5 des stations les plus saturées par jour
    """
    # On crée une fenêtre par jour, triée par station_saturee décroissant
    w = Window.partitionBy("date_key").orderBy(F.col("station_saturee").desc())

    # On garde uniquement le top 5 des stations les plus saturées par jour
    fact_top_5_station_saturee_per_day = (
        fact_station_saturee_per_day(
            df_join_availability_station, dim_date_df, dim_station_df
        )
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 5)
        .drop("rn")
    )
    return fact_top_5_station_saturee_per_day


# liste des 5 stations les plus saturées par jour


def fact_top5_array(
    df_join_availability_station: DataFrame,
    dim_date_df: DataFrame,
    dim_station_df: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits du top 5 des stations les plus saturées par jour sous forme de liste
    Colonnes:
    - date_key (int) : clé surrogate de la date
    - top_5_stations (array) : liste des 5 stations les plus saturées du jour

    Args:
        df_join_availability_station (DataFrame): dataframe de jointure (pour utiliser fact_top_5_station_saturee_per_day())
        dim_date_df (DataFrame): dataframe dimension date
        dim_station_df (DataFrame): dataframe dimension station

    Returns:
        fact_top5_array (DataFrame): table de faits du top 5 des stations les plus saturées par jour sous forme de liste
    """

    df_top5 = fact_top_5_station_saturee_per_day(
        df_join_availability_station, dim_date_df, dim_station_df
    )

    # Fenêtre partitionnée par date_key, triée par station_saturee desc, station_key asc (pour ordre stable)
    w = Window.partitionBy("date_key").orderBy(
        F.desc("station_saturee"), F.asc("station_key")
    )

    # Ajouter rang (row_number) pour prendre top 5 par date
    df_ranked = df_top5.withColumn("rank", F.row_number().over(w)).filter("rank <= 5")

    # Collecte ordonnée avec orderBy sur la clé date_key et rank (dans collect_list)
    fact_top5_array = (
        df_ranked.orderBy("date_key", "rank")
        .groupBy("date_key")
        .agg(
            F.collect_list(F.struct("station_key", "station_saturee")).alias(
                "top_5_stations"
            )
        )
    )

    return fact_top5_array


# endregion

# region DAILY GOLD
# --------------------


def availability_daily_gold(
    df_availability_silver: DataFrame,
    df_weather_silver: DataFrame,
    df_join_availability_station: DataFrame,
    dim_date_df: DataFrame,
    dim_station_df: DataFrame,
    dim_time_df: DataFrame,
    dim_weather_df: DataFrame,
) -> None:
    """
    Création de la table factuelle availability_daily_gold
    Colonnes:
    - date_key (int) : clé surrogate de la date
    - avg_bikes_available_day (float)
    - taux_occupation_day (float)
    - weather_condition_key_day (int) : clé surrogate de la condition météo dominante du jour
    - top_5_stations_day (array) : liste des 5 stations les plus saturées du jour

    Args:
        df_availability_silver (DataFrame): dataframe silver de la disponibilité des vélos
        df_weather_silver (DataFrame): dataframe silver des conditions météorologiques
        df_join_availability_station (DataFrame): dataframe résultant de la jointure entre la disponibilité des vélos et les informations des stations

    Returns:
        availability_daily_gold (DataFrame): table factuelle availability_daily_gold
    """
    availability_daily_gold = (
        (
            fact_avg_bikes_available_per_day_and_station(
                df_availability_silver, dim_date_df, dim_station_df
            )
            .join(
                fact_meteo_dominante(df_weather_silver, dim_date_df, dim_weather_df),
                "date_key",
                "inner",
            )
            .join(
                fact_taux_occupation(
                    df_join_availability_station,
                    dim_date_df,
                    dim_station_df,
                    dim_time_df,
                ),
                "date_key",
                "inner",
            )
            .join(
                fact_top5_array(
                    df_join_availability_station, dim_date_df, dim_station_df
                ),
                "date_key",
                "inner",
            )
        )
        .select(
            "date_key",
            "avg_bikes_available",
            "taux_occupation",
            "weather_condition_key",
            "top_5_stations",
        )
        .groupBy("date_key")
        .agg(
            F.round(F.avg("avg_bikes_available"), 2).alias("avg_bikes_available_day"),
            F.round(F.avg("taux_occupation"), 2).alias("taux_occupation_day"),
            F.first("weather_condition_key").alias("weather_condition_key_day"),
            F.first("top_5_stations").alias("top_5_stations_day"),
        )
        .orderBy("date_key")
    )
    return availability_daily_gold


# endregion


# region MAIN SCRIPT
# ------------------
if __name__ == "__main__":
    # creation Spark session
    spark = (
        SparkSession.builder.master("local[*]")
        # réduire le nombre de partitions de shuffle pour les petites données
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    # Connaitre le nombre de coeurs disponibles
    # print(f"==========================\nNOMBRE DE COEURS :{spark.sparkContext.defaultParallelism}\n==========================")

    # ==== DF SILVER ====

    # création des dataframes silver nécessaires
    df_availability_silver = create_silver_availability_df(spark)[0]
    df_weather_silver = create_silver_weather_df(spark)[0]
    df_station_silver = create_silver_station_df(spark)[0]

    # renommage des colonnes ayant le même nom dans différente tables
    df_availability_silver = df_availability_silver.withColumnRenamed(
        "date_partition", "availability_date_partition"
    ).withColumnRenamed("timestamp", "availability_timestamp")
    df_station_silver = df_station_silver.withColumnRenamed(
        "date_partition", "station_date_partition"
    )
    df_weather_silver = df_weather_silver.withColumnRenamed(
        "date_partition", "weather_date_partition"
    ).withColumnRenamed("timestamp", "weather_timestamp")

    # ==== DF JOINTURE ====

    # jointure des dataframes silver pour créer le dataframe gold quotidien
    df_join_availability_station = df_availability_silver.join(
        df_station_silver, "station_id", "inner"
    )
    # from_unixtime: transforme un timestamp ou string en nombre de secondes depuis 1970-01-01 00:00:00 UTC
    # / 3600 * 3600 arrondi à l'heure la plus proche
    # unix_timestamp: inverse de from_unixtime, transforme un nombre de secondes en timestamp (correpond aussi à 1970)
    # /!\ timestamp_rounded => string
    df_join_availability_station = df_join_availability_station.withColumn(
        "timestamp_rounded",
        F.from_unixtime(
            F.round(F.unix_timestamp(F.col("availability_timestamp")) / 3600) * 3600
        ),
    )
    # creation du dataframe gold (jointure de tous les df silver)
    df_join_silver = df_join_availability_station.join(
        df_weather_silver,
        df_join_availability_station["timestamp_rounded"]
        == df_weather_silver["weather_timestamp"],
        "left",
    )

    # === DF GOLD DAILY ====

    # /!\ si dataframe plus conséquent et réutilisation tables dimensionnelles /!\:
    # Création des tables dimensionnelles **avec cache**
    dim_station_df = dim_station(df_station_silver).cache()
    dim_weather_df = dim_weather(df_weather_silver).cache()
    dim_date_df = dim_date(spark).cache()
    dim_time_df = dim_time(spark).cache()

    # Forcer le calcul immédiat pour "remplir" le cache
    # spark=> lazy evaluation, les transformations ne sont pas exécutées tant qu'une action n'est pas appelée, .count() en est une peu couteuse en ressources
    dim_station_df.count()
    dim_weather_df.count()
    dim_date_df.count()
    dim_time_df.count()

    # création de la table factuelle gold_daily
    df_gold = availability_daily_gold(
        df_availability_silver,
        df_weather_silver,
        df_join_availability_station,
        dim_date_df,
        dim_station_df,
        dim_time_df,
        dim_weather_df,
    )

    # df_gold.show()
    df = fact_top5_array(df_join_availability_station, dim_date_df, dim_station_df)
    df.show()


# endregion

# spark-submit etl/gold/gold_daily.py
