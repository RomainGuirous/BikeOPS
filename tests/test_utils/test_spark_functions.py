# region IMPORTS
import pytest
from etl.utils.spark_functions import (
    read_csv_spark,
    apply_transformations,
    process_report_and_cleanup,
    dedupe_by_partition_order,
    create_silver_df,
    quality_rapport
)
# endregion


# region READ CSV


def test_read_csv_spark(spark_session: pytest.fixture):
    df = read_csv_spark(spark_session, "tests/common/data_tests/sample.csv")
    assert df is not None
    assert df.count() > 0


# endregion

# region APPLY TRANSFORMATIONS

# mark.parametrize: faire la ft plusieurs fois avec des paramètres différents (fixtures sont intégrées automatiquement)
# => request.getfixturevalue: permet d'obtenir dynamiquement la fixture attendue
@pytest.mark.parametrize(
    "score, drop, expected_df_name",
    [
        (False, None, "expected_df_fixture"),
        (True, None, "expected_df_fixture_with_score"),
        (False, ["fruit"], "expected_df_fixture_with_drop"),
    ]
)
def test_apply_transformations_param(input_df_fixture: pytest.fixture, transformations_fixture: pytest.fixture, score: bool, drop: list, expected_df_name: str, request: pytest.FixtureRequest):
    expected_df_fixture = request.getfixturevalue(expected_df_name)
    result_df = apply_transformations(input_df_fixture, transformations_fixture, score=score, drop=drop)

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_fixture.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region REPORT AND CLEANUP

def test_process_report_and_cleanup_df(expected_df_fixture, expected_df_after_cleanup_fixture):
    result_df, _ = process_report_and_cleanup(expected_df_fixture)

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_after_cleanup_fixture.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


def test_process_report_and_cleanup_report(expected_df_fixture, expected_report_fixture):
    _, report = process_report_and_cleanup(expected_df_fixture)

    assert report == expected_report_fixture


# endregion

# region DROP DUPLICATES

def test_dedupe_by_partition_order(input_df_fixture_for_dedupe, expected_df_fixture_with_score):
    result_df = dedupe_by_partition_order(
        input_df_fixture_for_dedupe,
        partition_cols=["id", "fruit"],
        order_col="score"
    )

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_fixture_with_score.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()
    