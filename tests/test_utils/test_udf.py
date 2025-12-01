# region IMPORTS
import pytest
from pyspark.sql import functions as F
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


def test_clean_date(
    df_date_input_fixture: pytest.fixture,
    df_date_output_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_date nettoie correctement les dates.
    """
    df_result = df_date_input_fixture.withColumn(
        "date_string", clean_date("date_string")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_date_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN TEMPERATURE


def test_clean_temperature(
    df_temperature_input_fixture: pytest.fixture,
    df_temperature_output_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_temperature nettoie correctement les températures.
    """
    df_result = df_temperature_input_fixture.withColumn(
        "temperature_string", clean_temperature("temperature_string")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_temperature_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN RAIN MM


def test_clean_rain_mm(
    df_rain_input_fixture: pytest.fixture, df_rain_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_rain_mm nettoie correctement les précipitations en mm.
    """
    df_result = df_rain_input_fixture.withColumn(
        "rain_string", clean_rain_mm("rain_string")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_rain_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN WEATHER


def test_clean_weather(
    df_weather_udf_input_fixture: pytest.fixture, df_weather_udf_output_fixture: pytest.fixture
):
    """
    Teste que les fonctions de nettoyage des données météo fonctionnent correctement ensemble.
    """
    df_result = df_weather_udf_input_fixture.withColumn(
        "weather_condition", clean_weather("weather_condition")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_weather_udf_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN LATITUDE


def test_clean_latitude(
    df_latitude_input_fixture: pytest.fixture,
    df_latitude_output_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_latitude nettoie correctement les latitudes.
    """
    df_result = df_latitude_input_fixture.withColumn(
        "latitude", clean_latitude("latitude")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_latitude_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN LONGITUDE


def test_clean_longitude(
    df_longitude_input_fixture: pytest.fixture,
    df_longitude_output_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_longitude nettoie correctement les longitudes.
    """
    df_result = df_longitude_input_fixture.withColumn(
        "longitude", clean_longitude("longitude")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_longitude_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN STATION NAME


def test_clean_station_name(
    df_station_name_input_fixture: pytest.fixture,
    df_station_name_expected_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_station_name nettoie correctement les noms de stations.
    """
    df_result = df_station_name_input_fixture.withColumn(
        "station_name", clean_station_name("station_name")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_station_name_expected_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN POSITIVE INT


def test_clean_positive_int(
    df_positive_int_input_fixture: pytest.fixture,
    df_positive_int_expected_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_positive_int nettoie correctement les entiers positifs.
    """
    df_result = df_positive_int_input_fixture.withColumn(
        "positive_int", clean_positive_int("positive_int")
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_positive_int_expected_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN NB BIKES


def test_clean_nb_bikes(
    df_nb_bikes_input_fixture: pytest.fixture,
    df_nb_bikes_output_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_nb_bikes nettoie correctement le nombre de vélos.
    """
    df_result = df_nb_bikes_input_fixture.withColumn(
        "nb_bikes",
        clean_nb_bikes(
            F.col("nb_bikes"),
            F.col("places_libres"),
            F.col("capacity"),
        ),
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_nb_bikes_output_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# pytest tests/test_utils/test_udf.py
