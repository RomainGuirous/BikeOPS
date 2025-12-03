# region IMPORTS
from datetime import datetime
import pytest
from pyspark.sql import types as T, Row
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
        ("2025-12-01", 101, 5),
        ("2025-12-01", 102, 7),
        ("2025-12-02", 101, 3),
        ("2025-12-01", 101, 7),
        ("2025-12-01", 101, 3),
    ]

    schema_input = T.StructType(
        [
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
            T.StructField("date_key", T.LongType()),
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
            T.StructField("station_key", T.LongType()),
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
            T.StructField("avg_bikes_available", T.DoubleType()),
            T.StructField("date_key", T.LongType()),
            T.StructField("station_key", T.LongType()),
        ]
    )

    data_output = [
        (5.00, 1000, 2000),
        (7.00, 1000, 2001),
        (3.00, 1001, 2000),
    ]

    return spark_session.createDataFrame(data_output, schema_output)


# endregion

# region FACT TAUX OCCUPATION


@pytest.fixture
def fact_taux_occupation_input_df_fixture(spark_session):
    data_input = [
        # station_id, availability_date_partition, availability_timestamp, bikes_available, slots_free
        (101, "2025-12-01", datetime(2025, 12, 1, 8, 30), 5, 15),
        (101, "2025-12-01", datetime(2025, 12, 1, 9, 15), 7, 13),
        (102, "2025-12-01", datetime(2025, 12, 1, 8, 30), 3, 17),
        (102, "2025-12-01", datetime(2025, 12, 1, 9, 15), 10, 10),
        # pour vérifier calcul taux_occupation sur même station/date mais différents horaires
        (101, "2025-12-02", datetime(2025, 12, 2, 8, 30), 4, 16),
        (101, "2025-12-02", datetime(2025, 12, 2, 9, 15), 6, 14),
    ]

    schema_input = T.StructType(
        [
            T.StructField("station_id", T.IntegerType()),
            T.StructField("availability_date_partition", T.StringType()),
            T.StructField("availability_timestamp", T.TimestampType()),
            T.StructField("bikes_available", T.IntegerType()),
            T.StructField("slots_free", T.IntegerType()),
        ]
    )

    return spark_session.createDataFrame(data_input, schema_input)


@pytest.fixture
def fact_taux_occupation_dim_time_input_df_fixture(spark_session):
    data_dim_time = [
        (8, 30, 5000),
        (9, 15, 5001),
    ]

    schema_dim_time = T.StructType(
        [
            T.StructField("hour", T.IntegerType()),
            T.StructField("minute", T.IntegerType()),
            T.StructField("time_key", T.LongType()),
        ]
    )

    return spark_session.createDataFrame(data_dim_time, schema_dim_time)


@pytest.fixture
def fact_taux_occupation_output_df_fixture(spark_session):
    data_output = [
        (25.0, 1000, 5000, 2000),
        (35.0, 1000, 5001, 2000),
        (15.0, 1000, 5000, 2001),
        (50.0, 1000, 5001, 2001),
        (20.0, 1001, 5000, 2000),
        (30.0, 1001, 5001, 2000),
    ]

    schema_output = T.StructType(
        [
            T.StructField("taux_occupation", T.DoubleType()),
            T.StructField("date_key", T.LongType()),
            T.StructField("time_key", T.LongType()),
            T.StructField("station_key", T.LongType()),
        ]
    )

    return spark_session.createDataFrame(data_output, schema_output)


# endregion

# region FACT METEO


@pytest.fixture
def fact_meteo_dominante_input_df_fixture(spark_session):
    data_input = [
        ("2025-12-01", "Sunny"),
        ("2025-12-01", "Rain"),
        ("2025-12-01", "Sunny"),
        ("2025-12-02", "Cloudy"),
        ("2025-12-02", "Cloudy"),
        ("2025-12-02", "Rain"),
    ]
    schema_input = T.StructType(
        [
            T.StructField("weather_date_partition", T.StringType()),
            T.StructField("weather_condition", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data_input, schema_input)


@pytest.fixture
def fact_meteo_dominante_dim_weather_input_df_fixture(spark_session):
    data_dim_weather = [
        ("Sunny", 2000),
        ("Rain", 2001),
        ("Cloudy", 2002),
    ]
    schema_dim_weather = T.StructType(
        [
            T.StructField("weather_condition", T.StringType()),
            T.StructField("weather_condition_key", T.LongType()),
        ]
    )
    return spark_session.createDataFrame(data_dim_weather, schema_dim_weather)


@pytest.fixture
def fact_meteo_dominante_output_df_fixture(spark_session):
    data_output = [
        (2, 1000, 2000),  # 2 occurrences de "Sunny" le 2025-12-01
        (2, 1001, 2002),  # 2 occurrences de "Cloudy" le 2025-12-02
    ]
    schema_output = T.StructType(
        [
            T.StructField("count", T.LongType(), False),
            T.StructField("date_key", T.LongType()),
            T.StructField("weather_condition_key", T.LongType()),
        ]
    )
    return spark_session.createDataFrame(data_output, schema_output)


# endregion

# region FACT STATION SATUREE


@pytest.fixture
def fact_station_saturee_input_df_fixture(spark_session):
    data_input = [
        # availability_date_partition, station_id, bikes_available, capacity, slots_free (ignored but kept for forme)
        ("2025-12-01", 101, 20, 20, 0),  # saturée
        ("2025-12-01", 101, 18, 20, 2),  # non saturée
        ("2025-12-01", 101, 20, 20, 0),  # saturée
        ("2025-12-01", 102, 15, 15, 0),  # saturée
        ("2025-12-01", 102, 14, 15, 1),  # non saturée
        ("2025-12-02", 101, 19, 20, 1),  # non saturée
        ("2025-12-02", 101, 20, 20, 0),  # saturée
    ]

    schema_input = T.StructType(
        [
            T.StructField("availability_date_partition", T.StringType()),
            T.StructField("station_id", T.IntegerType()),
            T.StructField("bikes_available", T.IntegerType()),
            T.StructField("capacity", T.IntegerType()),
            T.StructField("slots_free", T.IntegerType()),
        ]
    )

    return spark_session.createDataFrame(data_input, schema_input)


@pytest.fixture
def fact_station_saturee_output_df_fixture(spark_session):
    data_output = [
        (
            66.67,
            1000,
            2000,
        ),  # 2 saturées / 3 relevés = 66.67% pour station 101 le 2025-12-01
        (
            50.00,
            1000,
            2001,
        ),  # 1 saturée / 2 relevés = 50.00% pour station 102 le 2025-12-01
        (
            50.00,
            1001,
            2000,
        ),  # 1 saturée / 2 relevés = 50.00% pour station 101 le 2025-12-02
    ]

    schema_output = T.StructType(
        [
            T.StructField("station_saturee", T.DoubleType()),
            T.StructField("date_key", T.LongType()),
            T.StructField("station_key", T.LongType()),
        ]
    )

    return spark_session.createDataFrame(data_output, schema_output)


# endregion

# region TOP 5 STATIONS SATUREES


@pytest.fixture
def fact_top_5_station_saturee_input_df_fixture(spark_session):
    schema_input = T.StructType(
        [
            T.StructField("availability_date_partition", T.StringType()),
            T.StructField("station_id", T.IntegerType()),
            T.StructField("bikes_available", T.IntegerType()),
            T.StructField("capacity", T.IntegerType()),
            T.StructField(
                "slots_free", T.IntegerType()
            ),  # Présent car drop en sortie mais pas utilisé dans le calcul
        ]
    )

    data_input = [
        # jour 2025-12-01:
        # station 101, 4 relevés, 2 saturés (bikes_available == capacity)
        ("2025-12-01", 101, 10, 10, 0),
        ("2025-12-01", 101, 10, 10, 0),
        ("2025-12-01", 101, 8, 10, 2),
        ("2025-12-01", 101, 6, 10, 4),
        # station 102, 3 relevés, 1 saturé
        ("2025-12-01", 102, 12, 12, 0),
        ("2025-12-01", 102, 10, 12, 2),
        ("2025-12-01", 102, 8, 12, 4),
        # station 103, 4 relevés, 1 saturé
        ("2025-12-01", 103, 15, 15, 0),
        ("2025-12-01", 103, 10, 15, 5),
        ("2025-12-01", 103, 9, 15, 6),
        ("2025-12-01", 103, 7, 15, 8),
        # station 104, 3 relevés, 0 saturé
        ("2025-12-01", 104, 5, 10, 5),
        ("2025-12-01", 104, 4, 10, 6),
        ("2025-12-01", 104, 3, 10, 7),
        # station 105, 2 relevés, 0 saturé
        ("2025-12-01", 105, 6, 12, 6),
        ("2025-12-01", 105, 5, 12, 7),
        # jour 2025-12-02:
        # station 106,  3 relevés, 3 saturés
        ("2025-12-02", 106, 20, 20, 0),
        ("2025-12-02", 106, 20, 20, 0),
        ("2025-12-02", 106, 20, 20, 0),
        # station 102, 3 relevés, 2 saturés
        ("2025-12-02", 102, 12, 12, 0),
        ("2025-12-02", 102, 12, 12, 0),
        ("2025-12-02", 102, 9, 12, 3),
        # station 101, 2 relevés, 1 saturé
        ("2025-12-02", 101, 10, 10, 0),
        ("2025-12-02", 101, 6, 10, 4),
        # station 103, 1 relevé, 0 saturé
        ("2025-12-02", 103, 7, 15, 8),
        # station 105, 1 relevé, 0 saturé
        ("2025-12-02", 105, 6, 12, 6),
    ]

    return spark_session.createDataFrame(data_input, schema_input)


@pytest.fixture
def fact_top_5_station_saturee_dim_station_input_df_fixture(spark_session):
    data_dim_station = [
        (101, 2000),
        (102, 2001),
        (103, 2002),
        (104, 2003),
        (105, 2004),
        (106, 2005),
    ]
    schema_dim_station = T.StructType(
        [
            T.StructField("station_id", T.IntegerType()),
            T.StructField("station_key", T.LongType()),
        ]
    )
    return spark_session.createDataFrame(data_dim_station, schema_dim_station)


@pytest.fixture
def fact_top_5_station_saturee_output_df_fixture(spark_session):
    schema_output = T.StructType(
        [
            T.StructField("station_saturee", T.DoubleType()),
            T.StructField("date_key", T.LongType()),
            T.StructField("station_key", T.LongType()),
        ]
    )

    data_output = [
        # 2025-12-01, date_key = 1000
        (50.0, 1000, 2000),  # station 101: 2 saturées sur 4 relevés -> 50%
        (33.33, 1000, 2001),  # station 102: 1 saturée sur 3 relevés -> 33.33%
        (25.0, 1000, 2002),  # station 103: 1 saturée sur 4 relevés -> 25%
        (0.0, 1000, 2003),  # station 104: 0 saturée sur 3 relevés -> 0%
        (0.0, 1000, 2004),  # station 105: 0 saturée sur 2 relevés -> 0%
        # 2025-12-02, date_key = 1001
        (100.0, 1001, 2005),  # station 106: 3 saturées sur 3 relevés -> 100%
        (66.67, 1001, 2001),  # station 102: 2 saturées sur 3 relevés -> 66.67%
        (50.0, 1001, 2000),  # station 101: 1 saturée sur 2 relevés -> 50%
        (0.0, 1001, 2002),  # station 103: 0 saturée sur 1 relevé -> 0%
        (0.0, 1001, 2004),  # station 105: 0 saturée sur 1 relevé -> 0%
    ]

    return spark_session.createDataFrame(data_output, schema_output)


# endregion

# region FACT TOP 5 ARRAY


@pytest.fixture
def fact_top5_array_output_df_fixture(spark_session):
    schema_output = T.StructType(
        [
            T.StructField("date_key", T.LongType()),  # nullable True par défaut
            T.StructField(
                "top_5_stations",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("station_key", T.LongType()),
                            T.StructField("station_saturee", T.DoubleType()),
                        ]
                    ),
                    containsNull=False,  # ici important: le ArrayType doit être nullable=True
                ),
                nullable=False,  # le champ top_5_stations aussi nullable=True
            ),
        ]
    )

    data_output = [
        (
            1000,
            [
                {"station_key": 2000, "station_saturee": 50.0},
                {"station_key": 2001, "station_saturee": 33.33},
                {"station_key": 2002, "station_saturee": 25.0},
                {"station_key": 2003, "station_saturee": 0.0},
                {"station_key": 2004, "station_saturee": 0.0},
            ],
        ),
        (
            1001,
            [
                {"station_key": 2005, "station_saturee": 100.0},
                {"station_key": 2001, "station_saturee": 66.67},
                {"station_key": 2000, "station_saturee": 50.0},
                {"station_key": 2002, "station_saturee": 0.0},
                {"station_key": 2004, "station_saturee": 0.0},
            ],
        ),
    ]

    return spark_session.createDataFrame(data_output, schema_output)


# endregion

# # region AVAILABILITY DAILY GOLD


# @pytest.fixture
# def fact_availability_silver_input_df_fixture(spark_session):
#     data = [
#         # availability_date_partition, station_id, bikes_available, capacity, slots_free
#         ("2025-12-01", 101, 10, 10, 0),
#         ("2025-12-01", 101, 10, 10, 0),
#         ("2025-12-01", 101, 8, 10, 2),
#         ("2025-12-01", 101, 6, 10, 4),
#         ("2025-12-01", 102, 12, 12, 0),
#         ("2025-12-01", 102, 10, 12, 2),
#         ("2025-12-01", 102, 8, 12, 4),
#         ("2025-12-01", 103, 15, 15, 0),
#         ("2025-12-01", 103, 10, 15, 5),
#         ("2025-12-01", 103, 9, 15, 6),
#         ("2025-12-01", 103, 7, 15, 8),
#         ("2025-12-01", 104, 5, 10, 5),
#         ("2025-12-01", 104, 4, 10, 6),
#         ("2025-12-01", 104, 3, 10, 7),
#         ("2025-12-01", 105, 6, 12, 6),
#         ("2025-12-01", 105, 5, 12, 7),
#         ("2025-12-02", 106, 20, 20, 0),
#         ("2025-12-02", 106, 20, 20, 0),
#         ("2025-12-02", 106, 20, 20, 0),
#         ("2025-12-02", 102, 12, 12, 0),
#         ("2025-12-02", 102, 12, 12, 0),
#         ("2025-12-02", 102, 9, 12, 3),
#         ("2025-12-02", 101, 10, 10, 0),
#         ("2025-12-02", 101, 6, 10, 4),
#         ("2025-12-02", 103, 7, 15, 8),
#         ("2025-12-02", 105, 6, 12, 6),
#     ]

#     schema = T.StructType(
#         [
#             T.StructField("availability_date_partition", T.StringType()),
#             T.StructField("station_id", T.IntegerType()),
#             T.StructField("bikes_available", T.IntegerType()),
#             T.StructField("capacity", T.IntegerType()),
#             T.StructField("slots_free", T.IntegerType()),
#         ]
#     )

#     return spark_session.createDataFrame(data, schema)


# @pytest.fixture
# def fact_weather_silver_input_df_fixture(spark_session):
#     data = [
#         ("2024-01-01", "Sunny", 1, "2024-01-01"),
#         ("2024-01-02", "Rain", 2, "2024-01-02"),
#     ]

#     schema = T.StructType(
#         [
#             T.StructField("date", T.StringType()),
#             T.StructField("weather_condition", T.StringType()),
#             T.StructField("weather_condition_key", T.IntegerType()),
#             T.StructField("weather_date_partition", T.StringType()),
#         ]
#     )

#     return spark_session.createDataFrame(data, schema)


# @pytest.fixture
# def fact_join_availability_station_input_df_fixture(spark_session):
#     data = [
#         # availability_date_partition, station_id, bikes_available, capacity, slots_free, station_key
#         ("2025-12-01", 101, 10, 10, 0, 2000),
#         ("2025-12-01", 101, 10, 10, 0, 2000),
#         ("2025-12-01", 101, 8, 10, 2, 2000),
#         ("2025-12-01", 101, 6, 10, 4, 2000),
#         ("2025-12-01", 102, 12, 12, 0, 2001),
#         ("2025-12-01", 102, 10, 12, 2, 2001),
#         ("2025-12-01", 102, 8, 12, 4, 2001),
#         ("2025-12-01", 103, 15, 15, 0, 2002),
#         ("2025-12-01", 103, 10, 15, 5, 2002),
#         ("2025-12-01", 103, 9, 15, 6, 2002),
#         ("2025-12-01", 103, 7, 15, 8, 2002),
#         ("2025-12-01", 104, 5, 10, 5, 2003),
#         ("2025-12-01", 104, 4, 10, 6, 2003),
#         ("2025-12-01", 104, 3, 10, 7, 2003),
#         ("2025-12-01", 105, 6, 12, 6, 2004),
#         ("2025-12-01", 105, 5, 12, 7, 2004),
#         ("2025-12-02", 106, 20, 20, 0, 2005),
#         ("2025-12-02", 106, 20, 20, 0, 2005),
#         ("2025-12-02", 106, 20, 20, 0, 2005),
#         ("2025-12-02", 102, 12, 12, 0, 2001),
#         ("2025-12-02", 102, 12, 12, 0, 2001),
#         ("2025-12-02", 102, 9, 12, 3, 2001),
#         ("2025-12-02", 101, 10, 10, 0, 2000),
#         ("2025-12-02", 101, 6, 10, 4, 2000),
#         ("2025-12-02", 103, 7, 15, 8, 2002),
#         ("2025-12-02", 105, 6, 12, 6, 2004),
#     ]

#     schema = T.StructType(
#         [
#             T.StructField("availability_date_partition", T.StringType()),
#             T.StructField("station_id", T.IntegerType()),
#             T.StructField("bikes_available", T.IntegerType()),
#             T.StructField("capacity", T.IntegerType()),
#             T.StructField("slots_free", T.IntegerType()),
#             T.StructField("station_key", T.LongType()),
#         ]
#     )

#     return spark_session.createDataFrame(data, schema)


# @pytest.fixture
# def dim_time_df_fixture(spark_session):
#     data = [
#         (0, "00:00:00"),
#         (1, "01:00:00"),
#         (23, "23:00:00"),
#     ]

#     schema = T.StructType(
#         [
#             T.StructField("time_key", T.IntegerType()),
#             T.StructField("time", T.StringType()),
#         ]
#     )

#     return spark_session.createDataFrame(data, schema)


# @pytest.fixture
# def dim_weather_df_fixture(spark_session):
#     data = [
#         (1, "Rain"),
#         (2, "Cloudy"),
#     ]

#     schema = T.StructType(
#         [
#             T.StructField("weather_condition_key", T.IntegerType()),
#             T.StructField("weather_condition", T.StringType()),
#         ]
#     )

#     return spark_session.createDataFrame(data, schema)


# @pytest.fixture
# def availability_daily_gold_output_df_fixture(spark_session):
#     data = [
#         Row(
#             date_key=1000,
#             avg_bikes_available_day=8.15,
#             taux_occupation_day=26.67,
#             weather_condition_key_day=1,
#             top_5_stations_day=[
#                 Row(station_key=2000, station_saturee=50.0),
#                 Row(station_key=2001, station_saturee=33.33),
#                 Row(station_key=2002, station_saturee=25.0),
#                 Row(station_key=2003, station_saturee=0.0),
#                 Row(station_key=2004, station_saturee=0.0),
#             ],
#         ),
#         Row(
#             date_key=1001,
#             avg_bikes_available_day=11.0,
#             taux_occupation_day=55.56,
#             weather_condition_key_day=2,
#             top_5_stations_day=[
#                 Row(station_key=2005, station_saturee=100.0),
#                 Row(station_key=2001, station_saturee=66.67),
#                 Row(station_key=2000, station_saturee=50.0),
#                 Row(station_key=2002, station_saturee=0.0),
#                 Row(station_key=2004, station_saturee=0.0),
#             ],
#         ),
#     ]

#     schema = T.StructType(
#         [
#             T.StructField("date_key", T.LongType()),
#             T.StructField("avg_bikes_available_day", T.DoubleType()),
#             T.StructField("taux_occupation_day", T.DoubleType()),
#             T.StructField("weather_condition_key_day", T.IntegerType()),
#             T.StructField(
#                 "top_5_stations_day",
#                 T.ArrayType(
#                     T.StructType(
#                         [
#                             T.StructField("station_key", T.LongType()),
#                             T.StructField("station_saturee", T.DoubleType()),
#                         ]
#                     )
#                 ),
#             ),
#         ]
#     )

#     return spark_session.createDataFrame(data, schema)


# # endregion
