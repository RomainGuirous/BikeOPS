from pyspark.sql import SparkSession, functions as F, types as T
import os
from etl.udf import(
    clean_positive_int,
    clean_latitude,
    clean_longitude,
    clean_station_name,
)


if __name__ == "__main__":

    # creation Spark session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # lecture du fichier CSV
    df = spark.read.option("header", True) \
        .option("sep", ",") \
        .option("mode", "DROPMALFORMED") \
        .csv("./data_raw/stations.csv")
    
    # nettoyage des données
    df_clean = (
    df.withColumn("station_id", clean_positive_int(F.col("station_id")))
      .withColumn("station_name", clean_station_name(F.col("station_name")))
      .withColumn("lat", clean_latitude(F.col("lat")))
      .withColumn("lon", clean_longitude(F.col("lon")))
      .withColumn("capacity", clean_positive_int(F.col("capacity"))) # pour avoir format a-m-j
    )

    # creation du répertoire data_clean s'il n'existe pas
    os.makedirs("../data_clean/", exist_ok=True)

    # utiliser pandas pour créer csv
    pandas_df = df_clean.toPandas()
    pandas_df.to_csv("./data_clean/stations_silver.csv", index=False)


    # df_clean.write.mode("overwrite") \
    # .parquet("./data_clean/stations_silver")

    # arrêt de la session Spark
    spark.stop()

    # spark-submit etl/silver/station_silver.py