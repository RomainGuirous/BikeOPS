# region IMPORTS
import pytest
from pyspark.sql import types as T
# endregion


@pytest.fixture
def df_stations_input_fixture(spark_session):
    data = [
        (1, "1", "Lille - Station 01", "50.62925", "3.057256", "20"),
        (2, "-5", "Invalid Name", "91.0", "-181.0", "-10"),
        (3, "abc", "Lille - Station 02", "48.8566", "2.3522", "15"),
        (4, "3", None, "45.75", "4.85", "0"),
        (5, "4", "Lille - Station 03", None, "3.04", "30"),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_id", T.StringType()),
            T.StructField("station_name", T.StringType()),
            T.StructField("lat", T.StringType()),
            T.StructField("lon", T.StringType()),
            T.StructField("capacity", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_df_station_df_fixture(spark_session):
    data = [
        (1, 1, "Lille - Station 01", 50.62925, 3.057256, 20),
        (2, None, None, None, None, None),
        (3, None, "Lille - Station 02", 48.8566, 2.3522, 15),
        (4, 3, None, 45.75, 4.85, None),
        (5, 4, "Lille - Station 03", None, 3.04, 30),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_id", T.IntegerType()),
            T.StructField("station_name", T.StringType()),
            T.StructField("lat", T.FloatType()),
            T.StructField("lon", T.FloatType()),
            T.StructField("capacity", T.IntegerType()),

        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_df_station_rapport_fixture():
    return {
        "station_id": {"lignes_corrigees": 2, "valeurs_invalides": 1},
        "station_name": {"lignes_corrigees": 2, "valeurs_invalides": 1},
        "lat": {"lignes_corrigees": 2, "valeurs_invalides": 1},
        "lon": {"lignes_corrigees": 1, "valeurs_invalides": 1},
        "capacity": {"lignes_corrigees": 2, "valeurs_invalides": 1},
    }
