# region IMPORTS
import pytest
from pyspark.sql import types as T
# endregion


# region DF ENTREE CLEAN DATE


@pytest.fixture
def df_date_input_fixture(spark_session):
    data = [
        (1, "2025-11-01T10:00:00"),
        (2, "01/11/2025 10:00"),
        (3, "01.11.2025"),
        (4, "01-11-2025 10:00:00"),
        (5, "2025-11-01"),
        (6, "2025-11-01 10:00"),
        (7, "2025-11-01 10:00:00.123Z"),
        (8, ""),
        (9, None),
        (10, "not-a-date"),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("date_string", T.StringType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN DATE


@pytest.fixture
def df_date_output_fixture(spark_session):
    data = [
        (1, ("2025-11-01 10:00:00", True, False)),
        (2, ("2025-11-01 10:00:00", True, False)),
        (3, ("2025-11-01 00:00:00", True, False)),
        (4, ("2025-11-01 10:00:00", True, False)),
        (5, ("2025-11-01 00:00:00", True, False)),
        (6, ("2025-11-01 10:00:00", True, False)),
        (7, ("2025-11-01 10:00:00", True, False)),
        (8, (None, False, True)),
        (9, (None, False, True)),
        (10, (None, False, True)),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField(
                "date_string",
                T.StructType(
                    [
                        T.StructField("value", T.StringType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
                True,
            ),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN TEMPERATURE


@pytest.fixture
def df_temperature_input_fixture(spark_session):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("temperature_string", T.StringType()),
        ]
    )

    data = [
        (1, "23"),
        (2, "23,5"),
        (3, "-25"),
        (4, "70"),
        (5, "abc"),
        (6, ""),
        (7, None),
    ]

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN TEMPERATURE


@pytest.fixture
def df_temperature_output_fixture(spark_session):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField(
                "temperature_string",
                T.StructType(
                    [
                        T.StructField("value", T.DoubleType(), True),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
                True,
            ),
        ]
    )

    data = [
        (1, (23.0, False, False)),
        (2, (23.5, True, False)),
        (3, (None, False, True)),
        (4, (None, False, True)),
        (5, (None, False, True)),
        (6, (None, False, True)),
        (7, (None, False, True)),
    ]

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN RAIN MM


@pytest.fixture
def df_rain_input_fixture(spark_session):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField("rain_string", T.StringType()),
        ]
    )

    data = [
        (1, "12.5"),
        (2, "3,7"),
        (3, "-1.2"),
        (4, ""),
        (5, None),
        (6, "abc"),
        (7, "0"),
        (8, "25"),
    ]

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN RAIN MM


@pytest.fixture
def df_rain_output_fixture(spark_session):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), False),
            T.StructField(
                "rain_string",
                T.StructType(
                    [
                        T.StructField("value", T.DoubleType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
                True,
            ),
        ]
    )

    data = [
        (1, (12.5, False, False)),
        (2, (3.7, True, False)),
        (3, (None, False, True)),
        (4, (None, False, True)),
        (5, (None, False, True)),
        (6, (None, False, True)),
        (7, (0.0, False, False)),
        (8, (25.0, False, False)),
    ]

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN WEATHER


@pytest.fixture
def df_weather_udf_input_fixture(spark_session):
    data = [
        (1, "Rain"),  # valide
        (2, "Cloudy"),  # valide
        (3, "Clear"),  # valide
        (4, "Drizzle"),  # valide
        (5, "Fog"),  # valide
        (6, "Sunny"),  # invalide
        (7, ""),  # vide -> invalide
        (8, None),  # None -> invalide
        (9, "rain"),  # casse sensible -> invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("weather_condition", T.StringType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN WEATHER


@pytest.fixture
def df_weather_udf_output_fixture(spark_session):
    data = [
        (1, ("Rain", False, False)),
        (2, ("Cloudy", False, False)),
        (3, ("Clear", False, False)),
        (4, ("Drizzle", False, False)),
        (5, ("Fog", False, False)),
        (6, (None, False, True)),  # invalide
        (7, (None, False, True)),  # invalide
        (8, (None, False, True)),  # invalide
        (9, (None, False, True)),  # casse sensible invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "weather_condition",
                T.StructType(
                    [
                        T.StructField("value", T.StringType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
            ),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN LATITUDE


@pytest.fixture
def df_latitude_input_fixture(spark_session):
    data = [
        (1, "45.0"),
        (2, "-90"),
        (3, "90"),
        (4, "91"),
        (5, "-91"),
        (6, ""),
        (7, None),
        (8, "abc"),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("latitude", T.StringType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN LATITUDE


@pytest.fixture
def df_latitude_output_fixture(spark_session):
    data = [
        (1, (45.0, False, False)),
        (2, (-90.0, False, False)),
        (3, (90.0, False, False)),
        (4, (None, False, True)),
        (5, (None, False, True)),
        (6, (None, False, True)),
        (7, (None, False, True)),
        (8, (None, False, True)),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "latitude",
                T.StructType(
                    [
                        T.StructField("value", T.FloatType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
            ),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN LONGITUDE


@pytest.fixture
def df_longitude_input_fixture(spark_session):
    data = [
        (1, "45.0"),
        (2, "-180"),
        (3, "180"),
        (4, "181"),
        (5, "-181"),
        (6, ""),
        (7, None),
        (8, "abc"),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("longitude", T.StringType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN LONGITUDE


@pytest.fixture
def df_longitude_output_fixture(spark_session):
    data = [
        (1, (45.0, False, False)),
        (2, (-180.0, False, False)),
        (3, (180.0, False, False)),
        (4, (None, False, True)),
        (5, (None, False, True)),
        (6, (None, False, True)),
        (7, (None, False, True)),
        (8, (None, False, True)),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "longitude",
                T.StructType(
                    [
                        T.StructField("value", T.FloatType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
            ),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN STATION NAME


@pytest.fixture
def df_station_name_input_fixture(spark_session):
    data = [
        (1, "Lille - Station 01"),
        (2, "Lille Station 01"),
        (3, None),
        (4, "Paris - Station 01"),
        (5, "Lille - Station 123"),
        (6, ""),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_name", T.StringType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN STATION NAME


@pytest.fixture
def df_station_name_expected_fixture(spark_session):
    data = [
        (1, ("Lille - Station 01", False, False)),
        (2, (None, False, True)),
        (3, (None, False, True)),
        (4, (None, False, True)),
        (5, (None, False, True)),
        (6, (None, False, True)),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "station_name",
                T.StructType(
                    [
                        T.StructField("value", T.StringType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
            ),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN POSITIVE INT


@pytest.fixture
def df_positive_int_input_fixture(spark_session):
    data = [
        (1, "123"),
        (2, "0"),
        (3, "-10"),
        (4, None),
        (5, "abc"),
        (6, "1"),
        (7, "1.5"),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("positive_int", T.StringType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN POSITIVE INT


@pytest.fixture
def df_positive_int_expected_fixture(spark_session):
    data = [
        (1, (123, True, False)),
        (2, (None, False, True)),
        (3, (None, False, True)),
        (4, (None, False, True)),
        (5, (None, False, True)),
        (6, (1, True, False)),
        (7, (None, False, True)),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "positive_int",
                T.StructType(
                    [
                        T.StructField("value", T.IntegerType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
            ),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion


@pytest.fixture
def df_nb_bikes_input_fixture(spark_session):
    data = [
        (1, "5", "10", "20"),
        (2, "-1", "10", "20"),
        (3, "25", "10", "20"),
        (4, None, "10", "20"),
        (5, "abc", "10", "20"),
        (6, None, "15", "10"),
        (7, None, None, "20"),
        (8, None, "10", None),
        (9, None, None, None),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("nb_bikes", T.StringType()),
            T.StructField("places_libres", T.StringType()),
            T.StructField("capacity", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def df_nb_bikes_output_fixture(spark_session):
    data = [
        (1, (5, False, False), "10", "20"),
        (2, (10, True, False), "10", "20"),
        (3, (20, True, False), "10", "20"),
        (4, (10, True, False), "10", "20"),
        (5, (10, True, False), "10", "20"),
        (6, (0, True, False), "15", "10"),
        (7, (None, False, True), None, "20"),
        (8, (None, False, True), "10", None),
        (9, (None, False, True), None, None),
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField(
                "nb_bikes",
                T.StructType(
                    [
                        T.StructField("value", T.IntegerType()),
                        T.StructField("ligne_corrigee", T.BooleanType()),
                        T.StructField("ligne_invalide", T.BooleanType()),
                    ]
                ),
            ),
            T.StructField("places_libres", T.StringType()),
            T.StructField("capacity", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)
