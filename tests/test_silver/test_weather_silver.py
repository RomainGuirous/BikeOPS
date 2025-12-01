import pytest
from etl.silver.weather_silver import create_silver_weather_df


def test_create_silver_weather_df_dataframe(
    spark_session: pytest.fixture,
    df_weather_silver_input_fixture: pytest.fixture,
    df_weather_silver_output_df_fixture: pytest.fixture,
):
    """
    Teste que la fonction create_silver_station_df crée un DataFrame Spark nettoyé correct.
    """
    df_result, _ = create_silver_weather_df(
        spark_session, df_input=df_weather_silver_input_fixture
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_weather_silver_output_df_fixture.orderBy("id").drop("id")
    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


def test_create_silver_weather_df_rapport(
    spark_session: pytest.fixture,
    df_weather_silver_input_fixture: pytest.fixture,
    df_weather_silver_output_rapport_fixture: pytest.fixture,
):
    """
    Teste que la fonction create_silver_station_df crée un DataFrame Spark nettoyé correct.
    """
    _, rapport = create_silver_weather_df(
        spark_session,
        df_input=df_weather_silver_input_fixture,
    )

    assert rapport == df_weather_silver_output_rapport_fixture

# pytest tests/test_silver/test_weather_silver.py
