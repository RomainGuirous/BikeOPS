from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

capacity = (
        spark.read.option("header", True)
        .option("sep", ",")
        .option("mode", "DROPMALFORMED")
        .csv("./data/data_raw/stations.csv")
        .select(["station_id", "capacity"])
    )

capacity.show(5)