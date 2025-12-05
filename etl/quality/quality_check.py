from pyspark.sql import SparkSession, functions as F, DataFrame
from etl.utils.spark_functions import read_csv_spark


def count_negative_values(
    df: DataFrame, target_col: str, *group_cols: str
) -> DataFrame:
    """
    Compte le nombre de valeurs négatives dans une colonne cible, groupées par les colonnes spécifiées.

    Args:
        df (DataFrame): Le DataFrame Spark à analyser.
        target_col (str): La colonne cible dans laquelle compter les valeurs négatives.
        *group_cols (str): Les colonnes par lesquelles grouper les résultats.

    Returns:
        DataFrame: Un DataFrame contenant les colonnes de regroupement et le nombre de valeurs négatives dans la colonne cible.
    """
    return df.groupBy(*group_cols).agg(
        F.sum(F.when(F.col(target_col) < 0, 1).otherwise(0)).alias(
            f"nb_neg_{target_col}"
        )
    )


def coherence_station_capacity(
    df_availability: DataFrame, df_stations: DataFrame, *group_cols: str
) -> DataFrame:
    """
    Vérifie la cohérence entre le nombre de vélos disponibles et la capacité des stations.

    Args:
        df_availability (DataFrame): DataFrame Spark des données de disponibilité des vélos.
        df_stations (DataFrame): DataFrame Spark des données des stations.
        *group_cols (str): Les colonnes par lesquelles grouper les résultats.

    Returns:
        DataFrame: Un DataFrame contenant les enregistrements où le nombre de vélos disponibles dépasse la capacité de la station.
    """
    df = df_availability.join(
        df_stations.select("station_id", "capacity"),
        on="station_id",
        how="inner",
    ).select(
        "station_id", "bikes_available", "slots_free", "capacity", "date_partition"
    )

    condition_incoherent = (
        (F.col("bikes_available") + F.col("slots_free") != F.col("capacity"))
        | F.col("bikes_available").isNull()
        | F.col("slots_free").isNull()
        | F.col("capacity").isNull()
    )

    return df.groupBy(*group_cols).agg(
        F.sum(F.when(condition_incoherent, 1).otherwise(0)).alias("nb_incoherent_capacity")
    )





if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        # réduire le nombre de partitions de shuffle pour les petites données
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    availability = spark.read.parquet(
        "/app/data/data_clean/silver/availability_silver/"
    )
    weather = spark.read.parquet("/app/data/data_clean/silver/weather_silver/")
    stations = read_csv_spark(spark, "/app/data/data_raw/stations.csv", delimiter=",")

    availability.printSchema()
    availability.show(5)
    weather.printSchema()
    weather.show(5)
    stations.printSchema()
    stations.show(5)

# spark-submit etl/quality/quality_check.py
