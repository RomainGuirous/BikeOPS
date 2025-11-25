# region IMPORTS
from pyspark.sql import SparkSession, functions as F, DataFrame

from etl.silver.availability_silver import create_silver_availability_df
from etl.silver.weather_silver import create_silver_weather_df
from etl.silver.station_silver import create_silver_station_df
from pyspark.sql.window import Window
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
) -> DataFrame:
    """
    Création de la table dimensionnelle des dates
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

    Args:
        spark (SparkSession): session Spark
        start_date (str): date de début au format "YYYY-MM-DD"
        end_date (str): date de fin au format "YYYY-MM-DD"

    Returns:
        dim_date (DataFrame): table dimensionnelle des dates
    """
    # Calcul du nombre de jours entre start_date et end_date inclus
    num_days = spark.sql(
        f"SELECT datediff(to_date('{end_date}'), to_date('{start_date}')) + 1 AS diff"
    ).collect()[0]["diff"]

    dim_date = (
        spark.range(num_days)
        .withColumn("date", F.date_add(F.lit(start_date), F.col("id").cast("int")))
        .drop("id")
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn(
            "day_of_week", F.date_format("date", "E")
        )  # Jour abrégé (Mon, Tue...)
        # Jour de la semaine en nombre ISO (1=lundi, ..., 7=dimanche)
        .withColumn("day_of_week_num", ((F.dayofweek("date") + 5) % 7 + 1))
        .withColumn("is_weekend", (F.col("day_of_week_num") >= 6).cast("boolean"))
        .withColumn("week_of_year", F.weekofyear("date"))
    )
    return dim_date


# endregion

# region DIM TIME
# --------


def dim_time(spark: SparkSession) -> DataFrame:
    """
    Création de la table dimensionnelle du temps
    Colonnes:
    - hour (int)
    - minute (int)
    - second (int)
    - time_key (int)

    Args:
        spark (SparkSession): session Spark

    Returns:
        dim_time (DataFrame): table dimensionnelle du temps
    """
    dim_time = (
        spark.range(24 * 60 * 60)  # Nombre total de secondes dans une journée
        .withColumn("hour", (F.col("id") / 3600).cast("int"))
        .withColumn("minute", ((F.col("id") % 3600) / 60).cast("int"))
        .withColumn("second", (F.col("id") % 60).cast("int"))
        .withColumn("time_key", F.col("id"))  # id unique qui sert de clé surrogate
        .drop("id")
        .orderBy("hour", "minute", "second")
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
) -> DataFrame:
    """
    Création de la table de faits de la moyenne de vélos disponibles par jour et par station
    Colonnes:
    - availability_date_partition (date)
    - station_id (int)
    - avg_bikes_available (float)

    Args:
        df_availability_silver (DataFrame): dataframe silver de la disponibilité des vélos

    Returns:
        fact_avg_velo_dispo_per_day_and_station (DataFrame): table de faits de la moyenne de vélos disponibles par jour et par station
    """
    fact_avg_velo_dispo_per_day_and_station = (
        df_availability_silver.groupBy("availability_date_partition", "station_id")
        .agg(F.round(F.avg("bikes_available"), 2).alias("avg_bikes_available"))
        .orderBy("availability_date_partition", "station_id")
    )
    return fact_avg_velo_dispo_per_day_and_station


# endregion

# region TAUX OCCUPATION
# ---------------


def fact_taux_occupation(df_availability_silver: DataFrame) -> DataFrame:
    """
    Création de la table de faits du taux d'occupation des vélos
    Colonnes:
    - availability_date_partition (date)
    - station_id (int)
    - taux_occupation (float)

    Args:
        df_availability_silver (DataFrame): dataframe silver de la disponibilité des vélos

    Returns:
        fact_taux_occupation (DataFrame): table de faits du taux d'occupation des vélos
    """
    fact_taux_occupation = df_availability_silver.withColumn(
        "taux_occupation",
        F.round(
            F.col("bikes_available")
            / (F.col("bikes_available") + F.col("slots_free"))
            * 100,
            2,
        ),
    )
    return fact_taux_occupation


# endregion

# region METEO DOMINANTE
# ----------------------


def fact_meteo_dominante(df_weather_silver: DataFrame) -> DataFrame:
    """
    Création de la table de faits de la météo dominante par jour
    Colonnes:
    - weather_date_partition (date)
    - weather_condition (string)
    - weather_condition_key (long)

    Args:
        df_weather_silver (DataFrame): dataframe silver des conditions météorologiques

    Returns:
        fact_meteo_dominante (DataFrame): table de faits de la météo dominante par jour
    """
    weather_counts = df_weather_silver.groupBy(
        "weather_date_partition", "weather_condition"
    ).count()

    # On crée une fenêtre par jour, triée par count décroissant
    w = Window.partitionBy("weather_date_partition").orderBy(F.desc("count"))

    # On garde uniquement la condition météo la plus fréquente du jour
    fact_meteo_dominante = (
        weather_counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    return fact_meteo_dominante


# region TOP 5 STATIONS SATUREES
# -----------------------


def fact_station_saturee_per_day(df_join_availability_station: DataFrame) -> DataFrame:
    """
    Création de la table de faits du pourcentage de stations saturées par jour
    Colonnes:
    - availability_date_partition (date)
    - station_id (int)
    - station_saturee (float)

    Args:
        df_join_availability_station (DataFrame): dataframe résultant de la jointure entre la disponibilité des vélos et les informations des stations

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
        .orderBy("availability_date_partition", F.desc("station_saturee"))
        .select("availability_date_partition", "station_id", "station_saturee")
    )
    return fact_station_saturee_per_day


def fact_top_5_station_saturee_per_day(
    df_join_availability_station: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits du top 5 des stations les plus saturées par jour
    Colonnes:
    - availability_date_partition (date)
    - station_id (int)
    - station_saturee (float)

    Args:
        df_join_availability_station (DataFrame): dataframe de jointure (pour utiliser fact_station_saturee_per_day())

    Returns:
        fact_top_5_station_saturee_per_day (DataFrame): table de faits du top 5 des stations les plus saturées par jour
    """
    # On crée une fenêtre par jour, triée par station_saturee décroissant
    w = Window.partitionBy("availability_date_partition").orderBy(
        F.col("station_saturee").desc()
    )

    # On garde uniquement le top 5 des stations les plus saturées par jour
    fact_top_5_station_saturee_per_day = (
        fact_station_saturee_per_day(df_join_availability_station)
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 5)
        .drop("rn")
    )
    return fact_top_5_station_saturee_per_day


def fact_top5_array(
    df_join_availability_station: DataFrame,
) -> DataFrame:
    """
    Création de la table de faits avec le top 5 des stations les plus saturées par jour sous forme de liste
    Colonnes:
    - availability_date_partition (date)
    - top_5_stations (array)

    Args:
        df_join_availability_station (DataFrame): dataframe de jointure (pour utiliser fact_top_5_station_saturee_per_day())

    Returns:
        fact_top5_array (DataFrame): table de faits avec le top 5 des stations les plus saturées par jour sous forme de liste
    """
    fact_top5_array = (
        fact_top_5_station_saturee_per_day(df_join_availability_station)
        .groupBy("availability_date_partition")
        .agg(
            F.collect_list(F.struct("station_id", "station_saturee")).alias(
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
) -> None:
    """
    Création de la table factuelle availability_daily_gold
    Colonnes:
    - availability_date_partition (date)
    - avg_bikes_available_day (float)
    - taux_occupation_day (float)
    - weather_condition_day (string)
    - top_5_stations_day (array)

    Args:
        df_availability_silver (DataFrame): dataframe silver de la disponibilité des vélos
        df_weather_silver (DataFrame): dataframe silver des conditions météorologiques
        df_join_availability_station (DataFrame): dataframe résultant de la jointure entre la disponibilité des vélos et les informations des stations

    Returns:
        availability_daily_gold (DataFrame): table factuelle availability_daily_gold
    """
    availability_daily_gold = (
        (
            fact_avg_bikes_available_per_day_and_station(df_availability_silver)
            .join(
                fact_meteo_dominante(df_weather_silver),
                fact_avg_bikes_available_per_day_and_station(df_availability_silver)[
                    "availability_date_partition"
                ]
                == fact_meteo_dominante(df_weather_silver)["weather_date_partition"],
                "inner",
            )
            .join(
                fact_taux_occupation(df_join_availability_station),
                "availability_date_partition",
                "inner",
            )
            .join(
                fact_top5_array(df_join_availability_station),
                "availability_date_partition",
                "inner",
            )
        )
        .select(
            "availability_date_partition",
            "avg_bikes_available",
            "taux_occupation",
            "weather_condition",
            "top_5_stations",
        )
        .groupBy("availability_date_partition")
        .agg(
            F.round(F.avg("avg_bikes_available"), 2).alias("avg_bikes_available_day"),
            F.round(F.avg("taux_occupation"), 2).alias("taux_occupation_day"),
            F.first("weather_condition").alias("weather_condition_day"),
            F.first("top_5_stations").alias("top_5_stations_day"),
        )
    )
    return availability_daily_gold


# endregion


# region MAIN SCRIPT
# ------------------
if __name__ == "__main__":
    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

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
    df_gold = df_join_availability_station.join(
        df_weather_silver,
        df_join_availability_station["timestamp_rounded"]
        == df_weather_silver["weather_timestamp"],
        "left",
    )

    # création de la table factuelle gold_daily
    df_gold = availability_daily_gold(
        df_availability_silver, df_weather_silver, df_join_availability_station
    )

    df_gold.show()

    # spark-submit etl/gold/gold_daily.py

# endregion
