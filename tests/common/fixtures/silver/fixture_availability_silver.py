# region IMPORTS
import pytest
from pyspark.sql import types as T
# endregion


@pytest.fixture
def df_availability_input_fixture(spark_session):
    data = [
        (1, "2025-11-01 08:00:00", "5", "15"),
        (2, "2025-11-01 09:00:00", "-1", "15"),
        (3, "2025-11-01 10:00:00", "25", "0"),
        (4, "invalid_date", "abc", "20"),
        (5, "2025-11-01 12:00:00", None, None),
    ]
    schema = T.StructType(
        [
            T.StructField("station_id", T.StringType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("bikes_available", T.StringType()),
            T.StructField("slots_free", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def df_stations_capacity_fixture(spark_session):
    data = [
        (1, "20"),
        (2, "20"),
        (3, "20"),
        (4, "20"),
        (5, "30"),
    ]
    schema = T.StructType(
        [
            T.StructField("station_id", T.IntegerType()),
            T.StructField("capacity", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def df_availability_output_fixture(spark_session):
    from datetime import date

    data = [
        # station_id, timestamp (string), bikes_available, slots_free, date_partition
        (1, "2025-11-01 08:00:00", 5, 15, date(2025, 11, 1)),
        (
            2,
            "2025-11-01 09:00:00",
            5,
            15,
            date(2025, 11, 1),
        ),
        (
            3,
            "2025-11-01 10:00:00",
            20,
            0,
            date(2025, 11, 1),
        ),  # bikes_available corrigé à capacity=20, slots_free recalculé à 0
        (4, None, 0, 20, None),  # date invalide + valeurs invalides ("abc", "20")
        (
            5,
            "2025-11-01 12:00:00",
            None,
            None,
            date(2025, 11, 1),
        ),  # valeurs None mais date OK
    ]
    schema = T.StructType(
        [
            T.StructField("station_id", T.IntegerType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("bikes_available", T.IntegerType()),
            T.StructField("slots_free", T.IntegerType()),
            T.StructField("date_partition", T.DateType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def availability_rapport_value_fixture():
    return {
        "total_lignes_brutes": 5,
        "total_lignes_corrigees": 5,  # lignes 2,3,4 corrigées; ligne 5 partielle (date ok, valeurs None)
        "total_valeurs_invalides": 2,  # ligne 4 invalide (date + valeurs), ligne 5 valeurs None partiellement invalides mais date OK
        "total_lignes_supprimees": 0,  # aucune suppression
    }
