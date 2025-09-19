from pyspark.sql import SparkSession, functions as F, types as T
import pandas as pd
import os

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

@F.udf(T.DoubleType())
def clean_temperature(s):
    if not s:
        return None
    s = str(s).replace(",", ".")
    try:
        val = float(s)
        return val if (val >= -20 and val <= 50) else None
    except:
        return None
    
@F.udf(T.DoubleType())
def clean_rain_mm(s):
    if not s:
        return None
    s = str(s).replace(",", ".")
    try:
        val = float(s)
        return val if val >= 0 else None
    except:
        return None
    
@F.udf(T.StringType())
def clean_weather(s):
    if not s:
        return None
    try:
        return s if s in ['Rain', 'Cloudy', 'Clear', 'Drizzle', 'Fog'] else None
    except:
        return None
    

if __name__ == "__main__":

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.read.option("header", True) \
        .option("sep", ";") \
        .option("mode", "DROPMALFORMED") \
        .csv("../data_raw/weather_raw.csv")
    
    df_clean = df.withColumn("timestamp", clean_date("timestamp")) \
        .withColumn("temperature_c", clean_temperature("temperature_c")) \
        .withColumn("rain_mm", clean_rain_mm("rain_mm")) \
        .withColumn("weather_condition", clean_weather("weather_condition")) \

    pandas_df = df_clean.toPandas()
    os.makedirs("../data_clean/", exist_ok=True)

    pandas_df.to_csv("../data_clean/clean_weather.csv", index=False)

    spark.stop()

    #spark-submit clean_weather.py
