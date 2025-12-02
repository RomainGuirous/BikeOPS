# region IMPORTS
import pytest
from pyspark.sql import types as T
# endregion

# region DIM STATION


@pytest.fixture
def dim_station_input_df_fixture(spark_session):
    data = [
        (1, 101, "Paris - Station 101", 48.8566, 2.3522, 20),
        (2, 202, "Lyon - Station 202", 45.7640, 4.8357, 15),
        (3, 303, "Saint-Denis - Station 303", 48.9362, 2.3574, 10),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_id", T.IntegerType()),
            T.StructField("station_name", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("capacity", T.IntegerType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def dim_station_expected_df_fixture(spark_session):
    data = [
        (1, 101, "Paris - Station 101", 48.8566, 2.3522, 20, "Paris", "101", 101001),
        (2, 202, "Lyon - Station 202", 45.7640, 4.8357, 15, "Lyon", "202", 202002),
        (
            3,
            303,
            "Saint-Denis - Station 303",
            48.9362,
            2.3574,
            10,
            "Saint-Denis",
            "303",
            303003,
        ),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_id", T.IntegerType()),
            T.StructField("station_name", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("capacity", T.IntegerType()),
            T.StructField("city", T.StringType()),
            T.StructField("station_number", T.StringType()),
            T.StructField("station_key", T.LongType(), False),
            # station_key est généré dynamiquement, valeurs fictives dans les données ci-dessus
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DIM WEATHER


@pytest.fixture
def dim_weather_input_df_fixture(spark_session):
    data = [
        (1, "Sunny"),
        (2, "Rainy"),
        (3, "Cloudy"),
        (4, "Snowy"),
        (5, "Rainy"),  # doublon volontaire pour test dropDuplicates
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),  # id ajouté, nullable=True par défaut
            T.StructField("weather_condition", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def dim_weather_expected_df_fixture(spark_session):
    data = [
        (1, "Sunny", 0),
        (2, "Rainy", 1),
        (3, "Cloudy", 2),
        (4, "Snowy", 3),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("weather_condition", T.StringType()),
            T.StructField("weather_condition_key", T.LongType(), nullable=False),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region FACT AVG BIKES


@pytest.fixture
def fact_avg_bikes_input_df_fixture(spark_session):
    data_input = [
        (1, "2025-12-01", 101, 5),
        (2, "2025-12-01", 102, 7),
        (3, "2025-12-02", 101, 3),
    ]

    schema_input = T.StructType(
        [
            T.StructField("id", T.IntegerType()),  # nullable True par défaut
            T.StructField("availability_date_partition", T.StringType()),
            T.StructField("station_id", T.IntegerType()),
            T.StructField("bikes_available", T.IntegerType()),
        ]
    )

    return spark_session.createDataFrame(data_input, schema_input)


@pytest.fixture
def fact_avg_bikes_dim_date_input_df_fixture(spark_session):
    schema_dim_date = T.StructType(
        [
            T.StructField("date", T.StringType()),
            T.StructField("date_key", T.LongType(), nullable=False),
        ]
    )

    data_dim_date = [
        ("2025-12-01", 1000),
        ("2025-12-02", 1001),
    ]

    return spark_session.createDataFrame(data_dim_date, schema_dim_date)


@pytest.fixture
def fact_avg_bikes_dim_station_input_df_fixture(spark_session):
    schema_dim_station = T.StructType(
        [
            T.StructField("station_id", T.IntegerType()),
            T.StructField("station_key", T.LongType(), nullable=False),
        ]
    )

    data_dim_station = [
        (101, 2000),
        (102, 2001),
    ]

    return spark_session.createDataFrame(data_dim_station, schema_dim_station)


@pytest.fixture
def fact_avg_bikes_output_df_fixture(spark_session):
    schema_output = T.StructType(
        [
            T.StructField("date_key", T.LongType(), nullable=False),
            T.StructField("station_key", T.LongType(), nullable=False),
            T.StructField("avg_bikes_available", T.DoubleType()),
        ]
    )

    data_output = [
        (1000, 2000, 5.00),
        (1000, 2001, 7.00),
        (1001, 2000, 3.00),
    ]

    return spark_session.createDataFrame(data_output, schema_output)


# endregion
