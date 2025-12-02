# region IMPORTS
import pytest
from pyspark.sql import functions as F
from etl.gold.gold_daily import (
    dim_station,
    dim_weather,
    dim_date,
)
# endregion

def test_dim_station(
    spark_session: pytest.fixture,
    df_station_input_fixture: pytest.fixture,
    df_station_output_fixture: pytest.fixture,
):
    """
    Teste que la fonction dim_station crée un DataFrame Spark correct.
    """
    df_result = dim_station(spark_session, df_input=df_station_input_fixture)

    result_sorted = df_result.orderBy("station_id")
    expected_sorted = df_station_output_fixture.orderBy("station_id")
    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()