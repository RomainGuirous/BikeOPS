from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
import pandas as pd
import os

# le décorateur permet à Spark d'utiliser cette fonction
@F.udf(T.IntegerType())
def clean_positive_int(s):
    if not s:
        return None
    try:
        val = int(s)
        return val if val > 0 else None
    except:
        return None

@F.udf(T.StringType())
def clean_date(s):
    if not s:
        return None
    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isnull(d):
            return None
        return d.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None
    
@F.udf(T.IntegerType())
def clean_nb_bikes(a, b, capacity):
    if not a or not a.isnumeric():
        if b and capacity:
            try:
                diff = int(capacity) - int(b)
                return diff if diff >= 0 else 0
            except:
                return None
    try:
        val = int(a)
        if val < 0:
            return 0
        elif val > int(capacity):
            return int(capacity)
        else:
            return val
    except:
        return None
    

if __name__ == "__main__":

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.read.option("header", True) \
        .option("sep", ";") \
        .option("mode", "DROPMALFORMED") \
        .csv("../data_raw/availability_raw.csv")
    
    capacity =spark.read.option("header", True) \
        .option("sep", ",") \
        .option("mode", "DROPMALFORMED") \
        .csv("../data_clean/stations.csv") \
        .select("station_id", "capacity")
    
    df = df.join(capacity, on="station_id", how="left")
    
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
        .drop("capacity")
    
    dup_wind = Window.partitionBy("station_id", "timestamp").orderBy("score")

    df_dedup = df_clean.withColumn("rn", F.row_number().over(dup_wind)) \
                       .filter(F.col("rn") == 1) \
                       .drop("score", "rn")

    pandas_df = df_clean.toPandas()

    # pandas transforme les nulls en float
    for c in ("bikes_available", "slots_free"):
        if c in pandas_df.columns:
            pandas_df[c] = pandas_df[c].astype("Int64")

    os.makedirs("../data_clean/", exist_ok=True)

    pandas_df.to_csv("../data_clean/clean_availability.csv", index=False)

    spark.stop()

    #spark-submit clean_availability.py
