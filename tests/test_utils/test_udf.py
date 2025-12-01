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
    clean_date_input_fixture: pytest.fixture,
    clean_date_expected_fixture: pytest.fixture,
):
    """
    Teste que la fonction clean_date nettoie correctement les dates.
    """
    df_result = clean_date_input_fixture.withColumn(
        "cleaned",
        clean_date(
            "date_string",
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
    df_temperature_input_fixture: pytest.fixture, expected_temperature_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_temperature nettoie correctement les températures.
    """
    df_result = df_temperature_input_fixture.withColumn(
        "cleaned",
        clean_temperature(
            "temperature_string",
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
    expected_sorted = expected_temperature_output_fixture.orderBy("id").select(
        F.col("temperature_string"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN RAIN MM


def test_clean_rain_mm(
    df_rain_input_fixture: pytest.fixture, expected_rain_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_rain_mm nettoie correctement les précipitations en mm.
    """
    df_result = df_rain_input_fixture.withColumn(
        "cleaned",
        clean_rain_mm(
            "rain_string",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("rain_string"),  # .value => struct returned by UDF
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_rain_output_fixture.orderBy("id").select(
        F.col("rain_string"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN WEATHER


def test_clean_weather(
    df_weather_input_fixture: pytest.fixture, expected_weather_output_fixture: pytest.fixture
):
    """
    Teste que les fonctions de nettoyage des données météo fonctionnent correctement ensemble.
    """
    df_result = df_weather_input_fixture.withColumn(
        "cleaned",
        clean_weather(
            "weather_condition",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias(
            "weather_condition"
        ),  # .value => struct returned by UDF
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_weather_output_fixture.orderBy("id").select(
        F.col("weather_condition"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN LATITUDE


def test_clean_latitude(
    df_latitude_input_fixture: pytest.fixture, expected_latitude_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_latitude nettoie correctement les latitudes.
    """
    df_result = df_latitude_input_fixture.withColumn(
        "cleaned",
        clean_latitude(
            "latitude",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("latitude"),
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_latitude_output_fixture.orderBy("id").select(
        F.col("latitude"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN LONGITUDE


def test_clean_longitude(
    df_longitude_input_fixture: pytest.fixture, expected_longitude_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_longitude nettoie correctement les longitudes.
    """
    df_result = df_longitude_input_fixture.withColumn(
        "cleaned",
        clean_longitude(
            "longitude",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("longitude"),
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_longitude_output_fixture.orderBy("id").select(
        F.col("longitude"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN STATION NAME


def test_clean_station_name(
    df_station_name_input_fixture: pytest.fixture, df_station_name_expected_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_station_name nettoie correctement les noms de stations.
    """
    df_result = df_station_name_input_fixture.withColumn(
        "cleaned",
        clean_station_name(
            "station_name",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("station_name"),
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = df_station_name_expected_fixture.orderBy("id").select(
        F.col("station_name"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN POSITIVE INT


def test_clean_positive_int(
    df_positive_int_input_fixture: pytest.fixture, expected_positive_int_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_positive_int nettoie correctement les entiers positifs.
    """
    df_result = df_positive_int_input_fixture.withColumn(
        "cleaned",
        clean_positive_int(
            "positive_int",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("positive_int"),
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_positive_int_output_fixture.orderBy("id").select(
        F.col("positive_int"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region CLEAN NB BIKES


def test_clean_nb_bikes(
    df_nb_bikes_input_fixture: pytest.fixture, expected_nb_bikes_output_fixture: pytest.fixture
):
    """
    Teste que la fonction clean_nb_bikes nettoie correctement le nombre de vélos.
    """
    df_result = df_nb_bikes_input_fixture.withColumn(
        "cleaned",
        clean_nb_bikes(
            "nb_bikes",
            "places_libres",
            "capacity",
            "ligne_corrigee",
            "ligne_invalide",
        ),
    )

    result_sorted = df_result.orderBy("id").select(
        F.col("cleaned.value").alias("nb_bikes"),
        F.col("cleaned.ligne_corrigee").alias("ligne_corrigee"),
        F.col("cleaned.ligne_invalide").alias("ligne_invalide"),
    )
    expected_sorted = expected_nb_bikes_output_fixture.orderBy("id").select(
        F.col("nb_bikes"),
        F.col("ligne_corrigee"),
        F.col("ligne_invalide"),
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()

# endregion

# pytest tests/test_utils/test_udf.py
