# region IMPORTS
import pytest
from pyspark.sql import functions as F
from etl.utils.udf import (
    clean_date,
    clean_temperature,
    clean_rain_mm,
    clean_weather
)
# endregion

# region CLEAN DATE


def test_clean_date(
    clean_date_input_fixture: pytest.fixture,
    clean_date_expected_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_date nettoie correctement les dates.
    """
    df_result = clean_date_input_fixture.withColumn(
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
    expected_sorted = clean_date_expected_fixture.orderBy("value")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN TEMPERATURE


def test_clean_temperature(
    df_temperature_input: pytest.fixture, expected_temperature_output: pytest.fixture
):
    """
    Teste que la fonction clean_temperature nettoie correctement les températures.
    """
    df_result = df_temperature_input.withColumn(
        "cleaned",
        clean_temperature(
            F.col("temperature_string"),
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias(
            "temperature_string"
        ),  # .value => struct returned by UDF
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_temperature_output.orderBy("id").select(
        F.col("temperature_string"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN RAIN MM


def test_clean_rain_mm(
    df_rain_input: pytest.fixture, expected_rain_output: pytest.fixture
):
    """
    Teste que la fonction clean_rain_mm nettoie correctement les précipitations en mm.
    """
    df_result = df_rain_input.withColumn(
        "cleaned",
        clean_rain_mm(
            F.col("rain_string"),
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("rain_string"),  # .value => struct returned by UDF
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_rain_output.orderBy("id").select(
        F.col("rain_string"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN WEATHER


def test_clean_weather(df_weather_input: pytest.fixture, expected_weather_output: pytest.fixture):
    """
    Teste que les fonctions de nettoyage des données météo fonctionnent correctement ensemble.
    """
    df_result = df_weather_input.withColumn(
        "cleaned",
        clean_weather(
            F.col("weather_condition"),
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("weather_condition"),  # .value => struct returned by UDF
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_weather_output.orderBy("id").select(
        F.col("weather_condition"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion


# pytest tests/test_utils/test_udf.py
