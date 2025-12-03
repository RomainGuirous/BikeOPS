# region IMPORTS
import pytest
from etl.gold.gold_daily import (
    dim_station,
    dim_weather,
    fact_avg_bikes_available_per_day_and_station,
    fact_taux_occupation,
    fact_meteo_dominante,
    fact_station_saturee_per_day,
    fact_top_5_station_saturee_per_day,
    fact_top5_array,
    availability_daily_gold,
)
from pyspark.sql import Row
# endregion


# region DIM STATION
def test_dim_station(
    dim_station_input_df_fixture: pytest.fixture,
    dim_station_expected_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction dim_station crée un DataFrame Spark correct.
    """
    df_result = dim_station(dim_station_input_df_fixture)

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = dim_station_expected_df_fixture.orderBy("id").drop("id")

    # On ne peut pas tester directement la colonne station_key car elle est générée dynamiquement
    # on vérifie tout de même son existence et son type
    assert result_sorted.schema == expected_sorted.schema
    assert (
        result_sorted.drop("station_key").collect()
        == expected_sorted.drop("station_key").collect()
    )


# endregion

# region DIM WEATHER


def test_dim_weather(
    dim_weather_input_df_fixture: pytest.fixture,
    dim_weather_expected_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction dim_weather crée un DataFrame Spark correct.
    """
    df_result = dim_weather(dim_weather_input_df_fixture)

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = dim_weather_expected_df_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    # clé surrogate générée dynamiquement
    assert (
        result_sorted.drop("weather_condition_key").collect()
        == expected_sorted.drop("weather_condition_key").collect()
    )


# endregion

# region DIM DATE


def test_dim_date():
    pass


# endregion

# region DIM DATE


def test_dim_time():
    pass


# endregion

# region FACT AVG BIKES


def test_fact_avg_bikes_available_per_day_and_station(
    fact_avg_bikes_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_station_input_df_fixture: pytest.fixture,
    fact_avg_bikes_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction fact_avg_bikes_available_per_day_and_station crée un DataFrame Spark correct.
    """
    df_result = fact_avg_bikes_available_per_day_and_station(
        fact_avg_bikes_input_df_fixture,
        fact_avg_bikes_dim_date_input_df_fixture,
        fact_avg_bikes_dim_station_input_df_fixture,
    )

    result_sorted = df_result.orderBy("date_key", "avg_bikes_available", "station_key")
    expected_sorted = fact_avg_bikes_output_df_fixture.orderBy(
        "date_key", "avg_bikes_available", "station_key"
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region FACT TAUX OCCUPATION


def test_fact_taux_occupation(
    fact_taux_occupation_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_station_input_df_fixture: pytest.fixture,
    fact_taux_occupation_dim_time_input_df_fixture: pytest.fixture,
    fact_taux_occupation_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction fact_taux_occupation crée un DataFrame Spark correct.
    """
    df_result = fact_taux_occupation(
        fact_taux_occupation_input_df_fixture,
        fact_avg_bikes_dim_date_input_df_fixture,
        fact_avg_bikes_dim_station_input_df_fixture,
        fact_taux_occupation_dim_time_input_df_fixture,
    )

    result_sorted = df_result.orderBy(
        "date_key", "taux_occupation", "station_key", "time_key"
    )
    expected_sorted = fact_taux_occupation_output_df_fixture.orderBy(
        "date_key", "taux_occupation", "station_key", "time_key"
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region FACT METEO DOMINANTE


def test_fact_meteo_dominante(
    fact_meteo_dominante_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_meteo_dominante_dim_weather_input_df_fixture: pytest.fixture,
    fact_meteo_dominante_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction fact_meteo_dominante crée un DataFrame Spark correct.
    """
    df_result = fact_meteo_dominante(
        fact_meteo_dominante_input_df_fixture,
        fact_avg_bikes_dim_date_input_df_fixture,
        fact_meteo_dominante_dim_weather_input_df_fixture,
    )

    result_sorted = df_result.orderBy("date_key", "count", "weather_condition_key")
    expected_sorted = fact_meteo_dominante_output_df_fixture.orderBy(
        "date_key", "count", "weather_condition_key"
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region FACT STATIONS SATUREES


def test_fact_station_saturee_per_day(
    fact_station_saturee_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_station_input_df_fixture: pytest.fixture,
    fact_station_saturee_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction fact_station_saturee_per_day crée un DataFrame Spark correct.
    """
    df_result = fact_station_saturee_per_day(
        fact_station_saturee_input_df_fixture,
        fact_avg_bikes_dim_date_input_df_fixture,
        fact_avg_bikes_dim_station_input_df_fixture,
    )

    result_sorted = df_result.orderBy("date_key", "station_saturee", "station_key")
    expected_sorted = fact_station_saturee_output_df_fixture.orderBy(
        "date_key", "station_saturee", "station_key"
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region FACT TOP 5 STATIONS SATUREES


def test_fact_top_5_station_saturee_per_day(
    fact_top_5_station_saturee_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_top_5_station_saturee_dim_station_input_df_fixture: pytest.fixture,
    fact_top_5_station_saturee_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction fact_top_5_station_saturee_per_day crée un DataFrame Spark correct.
    """
    df_result = fact_top_5_station_saturee_per_day(
        fact_top_5_station_saturee_input_df_fixture,
        fact_avg_bikes_dim_date_input_df_fixture,
        fact_top_5_station_saturee_dim_station_input_df_fixture,
    )

    result_sorted = df_result.orderBy("date_key", "station_saturee", "station_key")
    expected_sorted = fact_top_5_station_saturee_output_df_fixture.orderBy(
        "date_key", "station_saturee", "station_key"
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region FACT TOP 5 ARRAY


def test_fact_top5_array(
    fact_top_5_station_saturee_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_top_5_station_saturee_dim_station_input_df_fixture: pytest.fixture,
    fact_top5_array_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction fact_top_5_station_saturee_per_day crée un DataFrame Spark correct.
    """
    df_result = fact_top5_array(
        fact_top_5_station_saturee_input_df_fixture,
        fact_avg_bikes_dim_date_input_df_fixture,
        fact_top_5_station_saturee_dim_station_input_df_fixture,
    )

    result_sorted = df_result.orderBy("date_key", "top_5_stations")
    expected_sorted = fact_top5_array_output_df_fixture.orderBy(
        "date_key", "top_5_stations"
    )

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region AVAILABILITY DAILY GOLD


# def test_availability_daily_gold(
#     fact_availability_silver_input_df_fixture: pytest.fixture,
#     fact_weather_silver_input_df_fixture: pytest.fixture,
#     fact_join_availability_station_input_df_fixture: pytest.fixture,
#     fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
#     fact_top_5_station_saturee_dim_station_input_df_fixture: pytest.fixture,
#     dim_time_df_fixture: pytest.fixture,
#     dim_weather_df_fixture: pytest.fixture,
#     availability_daily_gold_output_df_fixture: pytest.fixture,
# ):
#     """
#     Teste que la fonction availability_daily_gold crée un DataFrame Spark correct.
#     """
#     df_result = availability_daily_gold(
#         fact_availability_silver_input_df_fixture,
#         fact_weather_silver_input_df_fixture,
#         fact_join_availability_station_input_df_fixture,
#         fact_avg_bikes_dim_date_input_df_fixture,
#         fact_top_5_station_saturee_dim_station_input_df_fixture,
#         dim_time_df_fixture,
#         dim_weather_df_fixture,
#     )

#     result_sorted = df_result.orderBy("date_key")
#     expected_sorted = availability_daily_gold_output_df_fixture.orderBy("date_key")

#     assert result_sorted.schema == expected_sorted.schema
#     assert result_sorted.collect() == expected_sorted.collect()

  # remplace par le chemin réel de ton module, par ex "src.gold.availability"


def test_availability_daily_gold(monkeypatch, spark_session):

    MODULE_PATH = "etl.gold.gold_daily"
    # Mock des DataFrames en sortie des fonctions internes
    df_avg = spark_session.createDataFrame(
        [(1, 10.5, 100), (2, 8.7, 101)],
        ["date_key", "avg_bikes_available", "station_key"],
    )

    df_meteo = spark_session.createDataFrame(
        [(1, 5, 200), (2, 3, 201)],
        ["date_key", "count", "weather_condition_key"],
    )

    df_taux = spark_session.createDataFrame(
        [(1, 50.0, 10, 100), (2, 75.0, 11, 101)],
        ["date_key", "taux_occupation", "time_key", "station_key"],
    )

   df_top5 = spark_session.createDataFrame([
    (1, [Row(station_key=100, station_saturee=0.8)]),
    (2, [Row(station_key=101, station_saturee=0.9)]),
], ["date_key", "top_5_stations"])

    # Monkeypatch des fonctions appelées dans availability_daily_gold
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_avg_bikes_available_per_day_and_station",
        lambda *args, **kwargs: df_avg,
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_meteo_dominante",
        lambda *args, **kwargs: df_meteo,
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_taux_occupation",
        lambda *args, **kwargs: df_taux,
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_top5_array",
        lambda *args, **kwargs: df_top5,
    )

    # Données d’entrée fictives (peu importe ici, car on mocke tout)
    df_availability_silver = spark_session.createDataFrame([], schema="dummy STRING")
    df_weather_silver = spark_session.createDataFrame([], schema="dummy STRING")
    df_join_availability_station = spark_session.createDataFrame([], schema="dummy STRING")
    dim_date_df = spark_session.createDataFrame([], schema="dummy STRING")
    dim_station_df = spark_session.createDataFrame([], schema="dummy STRING")
    dim_time_df = spark_session.createDataFrame([], schema="dummy STRING")
    dim_weather_df = spark_session.createDataFrame([], schema="dummy STRING")

    # Exécution de la fonction testée
    result_df = availability_daily_gold(
        df_availability_silver,
        df_weather_silver,
        df_join_availability_station,
        dim_date_df,
        dim_station_df,
        dim_time_df,
        dim_weather_df,
    )

    # Assertions sur le schéma
    expected_cols = [
        "date_key",
        "avg_bikes_available_day",
        "taux_occupation_day",
        "weather_condition_key_day",
        "top_5_stations_day",
    ]
    assert set(result_df.columns) == set(expected_cols)

    # Assertions basiques sur le contenu (par exemple le nombre de lignes)
    assert result_df.count() == 2

    # Optionnel : check des valeurs pour la première ligne
    row = result_df.orderBy("date_key").first()
    assert row["date_key"] == 1
    assert abs(row["avg_bikes_available_day"] - 10.5) < 0.01


# endregion

# pytest tests/test_gold/test_gold_daily.py
