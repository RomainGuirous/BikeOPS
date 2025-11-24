from pyspark.sql import SparkSession, functions as F

from etl.silver.availability_silver import create_silver_availability_df
from etl.silver.weather_silver import create_silver_weather_df
from etl.silver.station_silver import create_silver_station_df
from pyspark.sql.window import Window


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

    # ===== CREATION TABLE DIMENSION ======
    # * les dimensions doivent êtres descriptives et stables
    # * les dimensions ne doivent pas contenir des mesures fluctuantes
    # * /!\ les dimensions doivent être utilisées pour filtrer ou agréger les faits /!\

    # DIM STATION
    # ------------

    # monotonically_increasing_id: façon optimisée de créer id dans système distribué
    # si id autoincrementé, toutes les partitions doivent être scannées pour connaître prochain id à chaque ajout
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
        .withColumn("station_key", F.monotonically_increasing_id())
    )
    # dim_station.show()

    # DIM DATE
    # ---------

    start_date = "2025-01-01"
    end_date = "2025-12-31"

    # Calcul du nombre de jours entre start_date et end_date inclus
    # .collect: liste de lignes du résultat de la requête
    # ligne: équivalent de dict => "col": "valeur"
    num_days = spark.sql(
        f"SELECT datediff(to_date('{end_date}'), to_date('{start_date}')) + 1 AS diff"
    ).collect()[0]["diff"]

    # spark.range(num_days): crée un DataFrame avec une colonne "id" allant de 0 à num_days-1
    # date_add: ajoute un nombre de jours à une date (ici start_date avec id généré par spark.range)
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
    # dim_date.show(10, truncate=False)
    # dim_date.orderBy(F.col("date").desc()).show(10)

    # DIM WEATHER
    # ------------

    dim_weather = (
        df_weather_silver.select("weather_condition")
        .dropDuplicates()
        .withColumn("weather_condition_key", F.monotonically_increasing_id())
        .orderBy("weather_condition_key")
    )
    # dim_weather.show()

    # DIM TIME
    # --------

    dim_time = (
        spark.range(24 * 60 * 60)  # Nombre total de secondes dans une journée
        .withColumn("hour", (F.col("id") / 3600).cast("int"))
        .withColumn("minute", ((F.col("id") % 3600) / 60).cast("int"))
        .withColumn("second", (F.col("id") % 60).cast("int"))
        .withColumn("time_key", F.col("id"))  # id unique qui sert de clé surrogate
        .drop("id")
        .orderBy("hour", "minute", "second")
    )
    # dim_time.show(10, truncate=False)

    # ===== CREATION TABLE FAITS ======
    # * les faits doivent contenir des mesures quantitatives
    # * les faits doivent contenir des clés étrangères vers les dimensions
    # * les faits doivent répondre à des questions métier précises

    # AVG BIKES AVAILABLE PER DAY AND STATION
    # ----------------------------------------

    fact_avg_velo_dispo_per_day_and_station = (
        df_availability_silver.groupBy("availability_date_partition", "station_id")
        .agg(F.round(F.avg("bikes_available"), 2).alias("avg_bikes_available"))
        .orderBy("availability_date_partition", "station_id")
    )
    # fact_avg_velo_dispo_per_day_and_station.printSchema()

    # TAUX OCCUPATION
    # ---------------

    fact_taux_occupation = df_availability_silver.withColumn(
        "taux_occupation",
        F.round(
            F.col("bikes_available")
            / (F.col("bikes_available") + F.col("slots_free"))
            * 100,
            2,
        ),
    )
    # fact_taux_occupation.printSchema()

    # METEO DOMINANTE PAR JOUR
    # ------------------------

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
    # fact_meteo_dominante.printSchema()

    # TOP 5 STATIONS SATUREES
    # -----------------------

    # calcul du pourcentage de saturation par station et par jour
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
    # fact_station_saturee_per_day.show()

    # On crée une fenêtre par jour, triée par station_saturee décroissant
    w1 = Window.partitionBy("availability_date_partition").orderBy(
        F.col("station_saturee").desc()
    )

    # On garde uniquement le top 5 des stations les plus saturées par jour
    fact_top_5_station_saturee_per_day = (
        fact_station_saturee_per_day.withColumn("rn", F.row_number().over(w1))
        .filter(F.col("rn") <= 5)
        .drop("rn")
    )
    # fact_top_5_station_saturee_per_day.printSchema()

    # on agrège les 5 stations saturées par jour dans une liste
    fact_top5_array = fact_top_5_station_saturee_per_day.groupBy(
        "availability_date_partition"
    ).agg(
        F.collect_list(F.struct("station_id", "station_saturee")).alias(
            "top_5_stations"
        )
    )
    # fact_top5_array.printSchema()

    # TABLE FINALE availability_daily_gold
    # ------------------------------------

    availability_daily_gold = (
        fact_avg_velo_dispo_per_day_and_station.join(
            fact_meteo_dominante,
            fact_avg_velo_dispo_per_day_and_station["availability_date_partition"]
            == fact_meteo_dominante["weather_date_partition"],
            "inner",
        )
        .join(fact_taux_occupation, "availability_date_partition", "inner")
        .join(
            fact_top5_array, "availability_date_partition", "inner"
        )
    ).select(
        "availability_date_partition",
        "avg_bikes_available",
        "taux_occupation",
        "weather_condition",
        "top_5_stations",
    ).groupBy("availability_date_partition").agg(
        F.round(F.avg("avg_bikes_available"), 2).alias("avg_bikes_available_day"),
        F.round(F.avg("taux_occupation"), 2).alias("taux_occupation_day"),
        F.first("weather_condition").alias("weather_condition_day"),
        F.first("top_5_stations").alias("top_5_stations_day"),
    )

    availability_daily_gold.show()


# df_gold.show()
# df_weather_silver.show(5)
# df_availability_silver.show()
# df_station_silver.show(5)
# df_availability_silver.printSchema()
# df_weather_silver.printSchema()
# df_station_silver.printSchema()

# root
#  |-- station_id: integer (nullable = true)
#  |-- availability_timestamp: string (nullable = true)
#  |-- bikes_available: integer (nullable = true)
#  |-- slots_free: integer (nullable = true)
#  |-- availability_date_partition: date (nullable = true)

# root
#  |-- weather_timestamp: string (nullable = true)
#  |-- temperature_c: double (nullable = true)
#  |-- rain_mm: double (nullable = true)
#  |-- weather_condition: string (nullable = true)
#  |-- weather_date_partition: date (nullable = true)

# root
#  |-- station_id: integer (nullable = true)
#  |-- station_name: string (nullable = true)
#  |-- lat: float (nullable = true)
#  |-- lon: float (nullable = true)
#  |-- capacity: integer (nullable = true)

# schema df_gold:
# root
#  |-- station_id: integer (nullable = true)
#  |-- availability_timestamp: string (nullable = true)
#  |-- bikes_available: integer (nullable = true)
#  |-- slots_free: integer (nullable = true)
#  |-- availability_date_partition: date (nullable = true)
#  |-- station_name: string (nullable = true)
#  |-- lat: float (nullable = true)
#  |-- lon: float (nullable = true)
#  |-- capacity: integer (nullable = true)
#  |-- timestamp_rounded: string (nullable = true)
#  |-- weather_timestamp: string (nullable = true)
#  |-- temperature_c: double (nullable = true)
#  |-- rain_mm: double (nullable = true)
#  |-- weather_condition: string (nullable = true)
#  |-- weather_date_partition: date (nullable = true)

# tables faits:

# root
#  |-- availability_date_partition: date (nullable = true)
#  |-- station_id: integer (nullable = true)
#  |-- avg_bikes_available: double (nullable = true)

# root
#  |-- station_id: integer (nullable = true)
#  |-- availability_timestamp: string (nullable = true)
#  |-- bikes_available: integer (nullable = true)
#  |-- slots_free: integer (nullable = true)
#  |-- availability_date_partition: date (nullable = true)
#  |-- taux_occupation: double (nullable = true)

# root
#  |-- weather_date_partition: date (nullable = true)
#  |-- weather_condition: string (nullable = true)
#  |-- count: long (nullable = false)

# root
#  |-- availability_date_partition: date (nullable = true)
#  |-- top_5_stations: array (nullable = false)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- station_id: integer (nullable = true)
#  |    |    |-- station_saturee: double (nullable = true)

# spark-submit etl/gold/gold_daily.py
