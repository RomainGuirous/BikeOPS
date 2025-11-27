# region IMPORTS
import pytest
from pyspark.sql import functions as F, types as T
from etl.utils.udf import (
    clean_date,
    clean_temperature,
    clean_rain_mm,
    clean_weather,
    clean_latitude,
    clean_longitude,
    clean_station_name,
    clean_positive_int,
    clean_nb_bikes,
)
# endregion

# region CLEAN DATE

def test_clean_date(clean_date_input_fixture: pytest.fixture, clean_date_expected_fixture: pytest.fixture):
    """
    Teste que la fonction clean_date nettoie correctement les dates.
    """
    df_input = clean_date_input_fixture
    df_expected = clean_date_expected_fixture

    df_result = df_input.withColumn(
        "cleaned",
        clean_date(
            F.col("date_string"),
            "ligne_corrigee",
            "ligne_invalide",
        ),
    ).select(
        F.col("cleaned.value").alias("value"),
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )

    result_sorted = df_result.orderBy("value")
    expected_sorted = df_expected.orderBy("value")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# pytest tests/test_utils/test_udf.py