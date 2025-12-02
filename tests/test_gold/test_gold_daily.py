# region IMPORTS
import pytest
from etl.gold.gold_daily import dim_station, dim_weather,fact_avg_bikes_available_per_day_and_station
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

def test_fact_avg_bikes_available_per_day_and_station(
    fact_avg_bikes_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_date_input_df_fixture: pytest.fixture,
    fact_avg_bikes_dim_station_input_df_fixture: pytest.fixture,
    fact_avg_bikes_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction dim_weather crée un DataFrame Spark correct.
    """
    df_result = fact_avg_bikes_available_per_day_and_station(fact_avg_bikes_input_df_fixture,
                                                             fact_avg_bikes_dim_date_input_df_fixture,
                                                             fact_avg_bikes_dim_station_input_df_fixture)

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = fact_avg_bikes_output_df_fixture.orderBy("id").drop("id")

    assert result_sorted.schema == expected_sorted.schema
    assert (
        result_sorted.drop("weather_condition_key").collect()
        == expected_sorted.drop("weather_condition_key").collect()
    )

# pytest tests/test_gold/test_gold_daily.py
