# region IMPORTS
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from datetime import date, datetime
# endregion


# region SPARK SESSION FIXTURE


# scope='session': la fixture est créée une seule fois pour tous les tests de la session
# autouse=True: la fixture est automatiquement utilisée par tous les tests sans avoir besoin de la mentionner explicitement
# session + True: une seule session Spark est créée sans être appelée explicitement
@pytest.fixture(scope="session", autouse=True)
def spark_session():
    """
    Fixture pour créer une session Spark utilisée dans les tests.
    """
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    yield spark
    spark.stop()


# endregion

# region DF ENTREE


@pytest.fixture
def input_df_fixture(spark_session):
    """
    DataFrame d'entrée pour les tests.
    """
    data = [
        (1, "apple"),
        (2, "banana"),
        (3, None),
        (4, "  "),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE(DEDUPLICATION)


@pytest.fixture
def input_df_fixture_for_dedupe(spark_session):
    """
    DataFrame d'entrée avec doublons pour tester la déduplication,
    compatible avec la fixture de sortie expected_df_fixture_with_score.
    """
    data = [
        (1, "orange", True, False, 2),
        (1, "orange", True, False, 0),  # Meilleur score, doit être gardée
        (2, "banana", False, False, 0),
        (3, None, False, True, 3),
        (3, None, False, True, 1),  # Meilleur score, doit être gardée
        (4, "", True, True, 2),
        (4, "", True, True, 1),  # Meilleur score, doit être gardée
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), nullable=True),
            T.StructField("fruit", T.StringType(), nullable=True),
            T.StructField("lignes_corrigees", T.BooleanType(), nullable=True),
            T.StructField("valeurs_invalides", T.BooleanType(), nullable=True),
            T.StructField("score", T.IntegerType(), nullable=False),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SILVER ENTREE


@pytest.fixture
def input_silver_df_fixture(spark_session):
    data = [
        (1, " apple ", "2025-11-01 10:00:00"),
        (2, "banana", "2025-11-01 10:01:00"),
        (3, None, "2025-11-01 10:02:00"),
        (4, "  ", "2025-11-01 10:03:00"),
        (5, "apple", "2025-11-01 10:02:00"),  # doublon possible pour test déduplication
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
            T.StructField("timestamp", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE(BASIQUE)


@pytest.fixture
def expected_df_fixture(spark_session):
    """
    DataFrame attendu après application des transformations sans options supplémentaires.
    """
    data = [
        (1, "orange", True, False),
        (2, "banana", False, False),
        (3, None, False, True),
        (4, "", True, True),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
            T.StructField("lignes_corrigees", T.BooleanType()),
            T.StructField("valeurs_invalides", T.BooleanType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE(SCORE)


@pytest.fixture
def expected_df_fixture_with_score(spark_session):
    data = [
        (1, "orange", True, False, 0),
        (2, "banana", False, False, 0),
        (3, None, False, True, 1),
        (4, "", True, True, 1),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), nullable=True),
            T.StructField("fruit", T.StringType(), nullable=True),
            T.StructField("lignes_corrigees", T.BooleanType(), nullable=True),
            T.StructField("valeurs_invalides", T.BooleanType(), nullable=True),
            T.StructField("score", T.IntegerType(), nullable=False),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE(DROP)


@pytest.fixture
def expected_df_fixture_with_drop(spark_session):
    """
    DataFrame attendu après application des transformations avec suppression de la colonne 'fruit'.
    """
    data = [
        (1, True, False),
        (2, False, False),
        (3, False, True),
        (4, True, True),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("lignes_corrigees", T.BooleanType()),
            T.StructField("valeurs_invalides", T.BooleanType()),
            # La colonne 'fruit' est supprimée
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE(APRES CLEANUP)


@pytest.fixture
def expected_df_after_cleanup_fixture(spark_session):
    """
    DataFrame attendu après suppression des colonnes de reporting par process_report_and_cleanup.
    """
    data = [
        (1, "orange"),
        (2, "banana"),
        (3, None),
        (4, ""),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE SILVER


@pytest.fixture
def expected_silver_df_fixture(spark_session):
    data = [
        (1, "orange", "2025-11-01 10:00:00"),
        (2, "banana", "2025-11-01 10:01:00"),
        (3, None, "2025-11-01 10:02:00"),
        (4, "", "2025-11-01 10:03:00"),
        (5, "orange", "2025-11-01 10:02:00"),  # apple remplacé par orange (transfo 2)
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
            T.StructField("timestamp", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_silver_df_with_score_fixture(spark_session):
    data = [
        (1, "orange", "2025-11-01 10:00:00", 0),
        (2, "banana", "2025-11-01 10:01:00", 0),
        (3, None, "2025-11-01 10:02:00", 1),  # None compte comme invalide
        (4, "", "2025-11-01 10:03:00", 1),  # chaîne vide compte aussi
        (5, "orange", "2025-11-01 10:02:00", 0),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("score", T.IntegerType(), nullable=False),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_silver_df_with_partition_fixture(spark_session):
    # data = [
    #     (1, "orange", datetime(2025, 11, 1, 10, 0, 0), date(2025, 11, 1)),
    #     (2, "banana", datetime(2025, 11, 1, 10, 1, 0), date(2025, 11, 1)),
    #     (3, None, datetime(2025, 11, 1, 10, 2, 0), date(2025, 11, 1)),
    #     (4, "", datetime(2025, 11, 1, 10, 3, 0), date(2025, 11, 1)),
    #     (5, "orange", datetime(2025, 11, 1, 10, 3, 0), date(2025, 11, 1)),
    # ]
    data =[
        (1, "orange", "2025-11-01 10:00:00", date(2025, 11, 1)),
        (2, "banana", "2025-11-01 10:01:00", date(2025, 11, 1)),
        (3, None, "2025-11-01 10:02:00", date(2025, 11, 1)),
        (4, "", "2025-11-01 10:03:00", date(2025, 11, 1)),
        (5, "orange", "2025-11-01 10:02:00", date(2025, 11, 1)),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("fruit", T.StringType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("date_partition", T.DateType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_silver_df_with_dedup_and_drop_fixture(spark_session):
    data = [
        (1, "2025-11-01 10:00:00"),
        (2, "2025-11-01 10:01:00"),
        (4, "2025-11-01 10:03:00"),
        (5, "2025-11-01 10:02:00"),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("timestamp", T.StringType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region LIST TRANSFORMATIONS


@pytest.fixture
def transformations_fixture(spark_session):
    """
    Liste des transformations à appliquer dans les tests.
    """

    def clean_trim(col):
        """
        UDF qui retourne un struct avec value, ligne_corrigee et ligne_invalide
        """

        @F.udf(
            returnType=T.StructType(
                [
                    T.StructField("value", T.StringType()),
                    T.StructField("ligne_corrigee", T.BooleanType()),
                    T.StructField("ligne_invalide", T.BooleanType()),
                ]
            )
        )
        def udf_fn(value):
            if value is None:
                return (None, False, True)
            trimmed = value.strip()
            ligne_corrigee = trimmed != value
            ligne_invalide = trimmed == ""
            return (trimmed, ligne_corrigee, ligne_invalide)

        return udf_fn(col)

    def replace_apple_with_orange(col):
        """
        UDF qui retourne un struct avec value, ligne_corrigee et ligne_invalide
        """

        @F.udf(
            returnType=T.StructType(
                [
                    T.StructField("value", T.StringType()),
                    T.StructField("ligne_corrigee", T.BooleanType()),
                    T.StructField("ligne_invalide", T.BooleanType()),
                ]
            )
        )
        def udf_fn(value):
            if value == "apple":
                return ("orange", True, False)
            return (value, False, False)

        return udf_fn(col)

    return [
        {"col": "fruit", "func": clean_trim},
        {"col": "fruit", "func": replace_apple_with_orange},
    ]


# endregion


# region REPORT


@pytest.fixture
def expected_report_fixture():
    """
    Rapport attendu correspondant à la fixture expected_df_fixture en entrée.
    """
    return {
        "total_lignes_corrigees": 2,  # True aux lignes 1 et 4
        "total_valeurs_invalides": 2,  # True aux lignes 3 et 4
    }


# endregion


# region QUALITY REPORT


@pytest.fixture
def expected_quality_report_fixture():
    return {
        "total_lignes_brutes": 10,
        "total_lignes_corrigees": 2,
        "total_valeurs_invalides": 1,
        "total_lignes_supprimees": 0,
    }


# endregion

# region QUALITY REPORT CONTENT


@pytest.fixture
def expected_report_content():
    return "\n".join(
        [
            "Rapport qualité - test_fichier",
            "-------------------------------------",
            "- Lignes brutes : 10",
            "- Lignes corrigées : 2",
            "- Lignes invalidées (remplacées par None) : 1",
            "- Lignes supprimées : 0",
        ]
    )


# endregion


@pytest.fixture
def expected_rapport_silver_df_fixture():
    return {
        "total_lignes_brutes": 4,
        "total_lignes_corrigees": 0,
        "total_valeurs_invalides": 2,
        "total_lignes_supprimees": 0,
    }
