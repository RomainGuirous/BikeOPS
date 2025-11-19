from pyspark.sql import SparkSession, functions as F

from etl.silver.availability_silver import create_silver_availability_df
from etl.silver.weather_silver import create_silver_weather_df
from etl.silver.station_silver import create_silver_station_df


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
    #* les dimensions doivent êtres descriptives et stables
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

    dim_date = (
        df_availability_silver.withColumn(
            "date", F.to_date(F.col("availability_timestamp"))
        )
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("day", F.dayofmonth(F.col("date")))
        .withColumn("hour", F.hour(F.col("availability_timestamp")))
        .withColumn("minute", F.minute(F.col("availability_timestamp")))
        .withColumn(
            "day_of_week", F.date_format(F.col("date"), "E")
        )  # E: format jour abrégé (EEEE pour complet)
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin("Sat", "Sun"), True).otherwise(False),
        )
        .select(
            "availability_timestamp",
            "date",
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "day_of_week",
            "is_weekend",
        )
        .dropDuplicates()
        .withColumn("date_key", F.monotonically_increasing_id())
    )
    # dim_date.show(20)

    # DIM WEATHER
    # ------------

    dim_weather = (
        df_weather_silver.select(
            "weather_timestamp", "temperature_c", "rain_mm", "weather_condition"
        )
        .withColumn("date", F.to_date(F.col("weather_timestamp")))
        .withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("day", F.dayofmonth(F.col("date")))
        .withColumn("hour", F.hour(F.col("weather_timestamp")))
        .dropDuplicates()
        .withColumn("weather_key", F.monotonically_increasing_id())
    )
    # dim_weather.show()

    # ===== CREATION TABLE DIMENSION ======

    # VELOS MOYEN DISPONIBLES (par station)
    # ------------------------------------------------

    # Table de faits : vélos moyens disponibles sur toute la période
    fact_avg_bikes_per_station = (
        df_availability_silver.groupBy("station_id")  # Regrouper par station
        .agg(
            F.avg(F.col("bikes_available")).alias("avg_bikes_available")
        )  # Moyenne des vélos disponibles
        .join(
            dim_station, "station_id", "inner"
        )  # Joindre avec dim_station pour enrichir avec les infos station
        .select(
            "station_name", "avg_bikes_available"
        )  # Sélectionner les colonnes pertinentes
    )
    fact_avg_bikes_per_station.show()

# df_gold.show()
# df_weather_silver.show(5)
# df_availability_silver.show()
# df_station_silver.show(5)
# df_gold.printSchema()

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

# spark-submit etl/gold/gold_daily.py
