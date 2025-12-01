import pytest
from etl.silver.availability_silver import create_silver_availability_df


def test_create_silver_availability_df_dataframe(
    spark_session: pytest.fixture,
    df_availability_input_fixture: pytest.fixture,
    df_availability_output_fixture: pytest.fixture,
    df_stations_capacity_fixture: pytest.fixture,
):
    """
    Teste que la fonction create_silver_station_df crée un DataFrame Spark nettoyé correct.
    """
    df_result, _ = create_silver_availability_df(
        spark_session, df_input=df_availability_input_fixture, df_join=df_stations_capacity_fixture
    )

    result_sorted = df_result.orderBy("id").drop("id")
    expected_sorted = df_availability_output_fixture.orderBy("id").drop("id")
    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


def test_create_silver_availability_df_rapport(
    spark_session: pytest.fixture,
    df_availability_input_fixture: pytest.fixture,
    availability_rapport_value_fixture: pytest.fixture,
    df_stations_capacity_fixture: pytest.fixture,
):
    """
    Teste que la fonction create_silver_station_df crée un DataFrame Spark nettoyé correct.
    """
    _, rapport = create_silver_availability_df(
        spark_session, df_input=df_availability_input_fixture, df_join=df_stations_capacity_fixture
    )

    assert rapport == availability_rapport_value_fixture


# pytest tests/test_silver/test_availability_silver.py