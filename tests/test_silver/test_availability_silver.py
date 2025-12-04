from etl.silver.availability_silver import create_silver_availability_df
from datetime import date
from pyspark.sql import types as T, DataFrame


# Mock fonction create_silver_df
def mock_create_silver_df(df, transformations, **kwargs):
    data = [
        (101, "2025-11-01 08:00:00", 5, 15, date(2025, 11, 1)),
        (101, "2025-11-01 09:00:00", 5, 15, date(2025, 11, 1)),
        (101, "2025-11-01 10:00:00", 20, 0, date(2025, 11, 1)),
        (101, None, 0, 20, None),
        (102, "2025-11-01 12:00:00", None, None, date(2025, 11, 1)),
    ]

    schema = T.StructType(
        [
            T.StructField("station_id", T.IntegerType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("bikes_available", T.IntegerType()),
            T.StructField("slots_free", T.IntegerType()),
            T.StructField("date_partition", T.DateType()),
        ]
    )

    df_result = df.sparkSession.createDataFrame(data, schema)

    rapport_mock = {
        "total_lignes_brutes": 42,
        "total_lignes_corrigees": 5,
        "total_valeurs_invalides": 3,
        "total_lignes_supprimees": 1,
    }
    return df_result, rapport_mock


# Tests pour la fonction create_silver_availability_df (dataframe)
def test_create_silver_availability_df_returns_dataframe(
    monkeypatch,
    spark_session,
    df_availability_input_fixture,
    df_stations_capacity_fixture,
):
    monkeypatch.setattr(
        "etl.silver.availability_silver.create_silver_df", mock_create_silver_df
    )

    df_result, _ = create_silver_availability_df(
        spark_session,
        df_availability_input_fixture,
        df_stations_capacity_fixture,
    )

    expected_columns = [
        "station_id",
        "timestamp",
        "bikes_available",
        "slots_free",
        "date_partition",
    ]

    # verification des colonnes du DataFrame résultat
    assert expected_columns == df_result.columns

    # verification du nombre de lignes du DataFrame résultat
    assert df_result.count() == mock_create_silver_df(df_result, None)[0].count()  # 5

    # verification des valeurs du DataFrame résultat (première ligne)
    rows = df_result.collect()
    first_row = rows[0]

    assert first_row["station_id"] == 101
    assert first_row["timestamp"] == "2025-11-01 08:00:00"
    assert first_row["bikes_available"] == 5
    assert first_row["slots_free"] == 15
    assert first_row["date_partition"] == date(2025, 11, 1)


# Tests pour la fonction create_silver_availability_df (rapport)
def test_create_silver_availability_df_returns_rapport(
    monkeypatch,
    spark_session,
    df_availability_input_fixture,
    df_stations_capacity_fixture,
):
    monkeypatch.setattr(
        "etl.silver.availability_silver.create_silver_df", mock_create_silver_df
    )

    _, rapport = create_silver_availability_df(
        spark_session,
        df_input=df_availability_input_fixture,
        df_join=df_stations_capacity_fixture,
    )

    # vérification type du rapport
    assert isinstance(rapport, dict)

    # vérification nombre d'éléments dans le rapport
    assert len(rapport) == len(
        mock_create_silver_df(df_availability_input_fixture, None)[1]
    )  # 4

    # vérification des valeurs du rapport
    assert rapport["total_lignes_brutes"] == 42
    assert rapport["total_lignes_corrigees"] == 5
    assert rapport["total_valeurs_invalides"] == 3
    assert rapport["total_lignes_supprimees"] == 1


# Verification appel par défaut des fichiers CSV
# (contenu df et rapport déjà vérifié dans les tests précédents)
def test_create_silver_availability_df_default_args(monkeypatch, spark_session):
    # Mock read_csv_spark
    def mock_read_csv(spark, path, *args, **kwargs):
        if "availability_raw.csv" in path:
            schema = T.StructType(
                [
                    T.StructField("station_id", T.StringType()),
                    T.StructField("timestamp", T.StringType()),
                    T.StructField("bikes_available", T.StringType()),
                    T.StructField("slots_free", T.StringType()),
                ]
            )
            data = [("101", "2025-11-01 08:00:00", "5", "15")]
            return spark.createDataFrame(data, schema)

        elif "stations.csv" in path:
            schema = T.StructType(
                [
                    T.StructField("station_id", T.StringType()),
                    T.StructField("capacity", T.StringType()),
                ]
            )
            data = [("101", "20")]
            return spark.createDataFrame(data, schema)

        else:
            raise ValueError(f"Unexpected path {path}")

    monkeypatch.setattr("etl.silver.availability_silver.read_csv_spark", mock_read_csv)
    monkeypatch.setattr(
        "etl.silver.availability_silver.create_silver_df", mock_create_silver_df
    )

    df_result, rapport = create_silver_availability_df(spark_session)

    assert isinstance(df_result, DataFrame)
    assert isinstance(rapport, dict)


# pytest tests/test_silver/test_availability_silver.py
