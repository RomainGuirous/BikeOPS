# region IMPORTS
import pytest
from pyspark.sql import types as T
from datetime import date
# endregion


@pytest.fixture
def df_weather_silver_input_fixture(spark_session):
    data = [
        (1, "2025-12-01 12:00:00", "15.5", "0.0", "Sunny"),
        (2, "2025-12-01 13:00:00", "-5.2", "2.3", "Rain"),
        (3, "invalid_date", "20.1", "abc", None),
        (4, None, None, None, "Cloudy"),
        (5, "2025-12-01 15:00:00", "not_a_number", "0.0", "Foggy"),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("temperature_c", T.StringType()),
            T.StructField("rain_mm", T.StringType()),
            T.StructField("weather_condition", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def df_weather_silver_output_df_fixture(spark_session):
    data = [
        (1, "2025-12-01 12:00:00", 15.5, 0.0, None, date(2025, 12, 1)),
        (2, "2025-12-01 13:00:00", -5.2, 2.3, "Rain", date(2025, 12, 1)),
        (3, None, 20.1, None, None, None),
        (4, None, None, None, "Cloudy", None),
        (5, "2025-12-01 15:00:00", None, 0.0, None, date(2025, 12, 1)),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("temperature_c", T.DoubleType()),
            T.StructField("rain_mm", T.DoubleType()),
            T.StructField("weather_condition", T.StringType()),
            T.StructField("date_partition", T.DateType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def df_weather_silver_output_rapport_fixture():
    return {
        "total_lignes_brutes": 5,
        "total_lignes_corrigees": 0, # changer le type ne compte pas, ainsi que ajout date_partition
        "total_valeurs_invalides": 4,
        "total_lignes_supprimees": 0,
    }


# pytest tests/test_silver/test_weather_silver.py
