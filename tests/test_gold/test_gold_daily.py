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
    dim_date,
    dim_time
)
from pyspark.sql import Row, functions as F
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

# Ici, ne pas comparer sur 365 jours !!!
# Juste vérifier quelques dates critiques pour s'assurer que les colonnes sont bien calculées.
# La logique repose sur des ft natives Spark et Pandas, pas besoin de le tester.
# Ce qu'il faut vérifier, c'est que le DataFrame est bien généré avec les bonnes colonnes, types et les noms adéquats.
def test_dim_date(spark_session):
    df = dim_date(spark_session, "2025-01-01", "2025-01-03")

    # --------- structure ------
    expected_cols = [
        "date",
        "year",
        "month",
        "day",
        "quarter",
        "day_of_week",
        "day_of_week_num",
        "is_weekend",
        "week_of_year",
        "iso_year",
        "date_key",
    ]
    assert df.columns == expected_cols

    # -------- nombre de lignes ------
    assert df.count() == 3

    # -------- premières lignes : valeurs critiques ------
    rows = df.orderBy("date").collect()

    # 2025-01-01
    r0 = rows[0]
    assert str(r0["date"]) == "2025-01-01"
    assert r0["year"] == 2025
    assert r0["month"] == 1
    assert r0["day"] == 1
    assert r0["quarter"] == 1
    assert r0["day_of_week"] == "Wed"
    assert r0["day_of_week_num"] == 3
    assert r0["is_weekend"] is False
    assert r0["week_of_year"] == 1
    assert r0["iso_year"] == 2025
    assert isinstance(r0["date_key"], int)

    # 2025-01-03
    r2 = rows[2]
    assert str(r2["date"]) == "2025-01-03"
    assert r2["day_of_week"] == "Fri"
    assert r2["is_weekend"] is False


# endregion

# region DIM TIME

# Ici, ne pas comparer sur 1440 lignes !!!
# Juste vérifier quelques heures et minutes clés pour s'assurer que les colonnes sont bien calculées.
# On vérifie nombre de lignes, structure et quelques valeurs critiques.

def test_dim_time(spark_session):
    df = dim_time(spark_session)

    # Vérification de la structure et du nombre de lignes
    expected_cols = ["hour", "minute", "time_key"]
    assert df.columns == expected_cols
    assert df.count() == 24 * 60

    # Vérifier que "time_key" est bien calculé comme hour * 60 + minute
    def assert_time_key_correct(time_key_val):
        row = df.filter(F.col("time_key") == time_key_val).collect()[0]
        assert row["time_key"] == row["hour"] * 60 + row["minute"]

    # Tests spécifiques
    row0 = df.filter(F.col("time_key") == 0).collect()[0]
    assert row0["hour"] == 0
    assert row0["minute"] == 0

    row_mid = df.filter(F.col("time_key") == 720).collect()[0]
    assert row_mid["hour"] == 12
    assert row_mid["minute"] == 0

    row_last = df.filter(F.col("time_key") == 1439).collect()[0]
    assert row_last["hour"] == 23
    assert row_last["minute"] == 59

    # Validation de la cohérence
    for time_key_val in [0, 720, 1439, 123, 999]:
        assert_time_key_correct(time_key_val)


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

# But de ce test: vérifier que la fonction availability_daily_gold intègre correctement les différentes
# fact tables et produit un DataFrame avec le bon schéma et des valeurs attendues.
# NE PAS FAIRE: tester la logique interne des fonctions fact_* ici, déjà testé dans les tests unitaires précédents.
# On va mocker les fonctions fact_* pour retourner des DataFrames simples et contrôlés. (monkeypatching)
# Mocker une fonction = remplacer son comportement réel par un comportement artificiel, contrôlé, uniquement pendant le test.


def test_availability_daily_gold(monkeypatch, spark_session):
    # Mock des DataFrames en sortie des fonctions internes

    # Chemin du module où se trouvent les fonctions à mocker
    MODULE_PATH = "etl.gold.gold_daily"

    # On va retourner des DataFrames simples pour vérifier l'intégration.
    # Ces DataFrames mockés doivent correspondre au schéma des fonctions originales. (colonnes et types)
    # Valeurs choisies arbitrairement pour le test. (simples et distinctes)

    # mock fact_avg_bikes_available_per_day_and_station
    df_avg = spark_session.createDataFrame(
        [(10.5, 1, 100), (8.7, 2, 101)],
        ["avg_bikes_available", "date_key", "station_key"],
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_avg_bikes_available_per_day_and_station",
        lambda *args, **kwargs: df_avg,
    )

    # mock fact_meteo_dominante
    df_meteo = spark_session.createDataFrame(
        [(5, 1, 200), (3, 2, 201)],
        ["count", "date_key", "weather_condition_key"],
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_meteo_dominante",
        lambda *args, **kwargs: df_meteo,
    )

    # mock fact_taux_occupation
    df_taux = spark_session.createDataFrame(
        [(50.0, 1, 10, 100), (75.0, 2, 11, 101)],
        ["taux_occupation", "date_key", "time_key", "station_key"],
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_taux_occupation",
        lambda *args, **kwargs: df_taux,
    )

    # mock fact_top5_array
    df_top5 = spark_session.createDataFrame(
        [
            (1, [Row(station_key=100, station_saturee=0.8)]),
            (2, [Row(station_key=101, station_saturee=0.9)]),
        ],
        ["date_key", "top_5_stations"],
    )
    monkeypatch.setattr(
        f"{MODULE_PATH}.fact_top5_array",
        lambda *args, **kwargs: df_top5,
    )

    # Données d’entrée fictives (peu importe ici, car on mocke tout)
    # Doivent être fournis pour respecter la signature de la fonction availability_daily_gold
    df_availability_silver = spark_session.createDataFrame([], schema="dummy STRING")
    df_weather_silver = spark_session.createDataFrame([], schema="dummy STRING")
    df_join_availability_station = spark_session.createDataFrame(
        [], schema="dummy STRING"
    )
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
    assert result_df.columns == expected_cols

    # Assertions basiques sur le contenu (par exemple le nombre de lignes)
    assert result_df.count() == 2

    # Vérifier une ligne spécifique pour s'assurer que les jointures et agrégations se sont bien passées
    # Pas besoin de vérifier toutes les lignes, la logique interne est testée ailleurs
    row = result_df.orderBy("date_key").first()

    assert row["date_key"] == 1
    assert row["avg_bikes_available_day"] == 10.5
    assert row["taux_occupation_day"] == 50.0
    assert row["weather_condition_key_day"] == 200
    assert row["top_5_stations_day"] == [Row(station_key=100, station_saturee=0.8)]


# endregion

# pytest tests/test_gold/test_gold_daily.py
