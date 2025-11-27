# region IMPORTS
import pytest
from etl.utils.spark_functions import (
    read_csv_spark,
    apply_transformations,
    process_report_and_cleanup,
    dedupe_by_partition_order,
    quality_rapport,
    create_silver_df,
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
    ],
)
def test_apply_transformations_param(
    input_df_fixture: pytest.fixture,
    transformations_fixture: pytest.fixture,
    score: bool,
    drop: list,
    expected_df_name: str,
    request: pytest.FixtureRequest,
):
    """
    Teste que le DataFrame retourné par apply_transformations correspond au DataFrame attendu
    pour différents paramètres de score et drop."""
    expected_df_fixture = request.getfixturevalue(expected_df_name)
    result_df = apply_transformations(
        input_df_fixture, transformations_fixture, score=score, drop=drop
    )

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_fixture.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region REPORT AND CLEANUP


def test_process_report_and_cleanup_df(
    expected_df_fixture, expected_df_after_cleanup_fixture
):
    """
    Teste que le DataFrame retourné par process_report_and_cleanup correspond au DataFrame attendu après nettoyage.
    """
    result_df, _ = process_report_and_cleanup(expected_df_fixture)

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_after_cleanup_fixture.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


def test_process_report_and_cleanup_report(
    expected_df_fixture, expected_report_fixture
):
    """
    Teste que le rapport retourné par process_report_and_cleanup correspond au rapport attendu.
    """
    _, report = process_report_and_cleanup(expected_df_fixture)

    assert report == expected_report_fixture


# endregion

# region DROP DUPLICATES


def test_dedupe_by_partition_order(
    input_df_fixture_for_dedupe, expected_df_fixture_with_score
):
    """
    Teste la suppression des doublons (lignes ayant les mêmes valeurs dans partition_cols) ayant un score de plus de 1.
    """
    result_df = dedupe_by_partition_order(
        input_df_fixture_for_dedupe, partition_cols=["id", "fruit"], order_col="score"
    )

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_fixture_with_score.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion

# region QUALITY REPORT


def test_quality_rapport_creation_dossier_et_fichier(
    expected_quality_report_fixture: pytest.fixture, tmp_path: pytest.fixture
):
    """
    Teste que le dossier et le fichier rapport sont créés correctement.
    Et que le chemin est correct.
    """
    # tmp_path: dossier temporaire unique pour ce test
    # => pour l'utiliser: créer un sous-dossier ou un fichier dedans
    base_path = tmp_path / "app/data/data_clean/rapport_qualite"

    # --- Exécution de la fonction ---
    quality_rapport(
        expected_quality_report_fixture, "test_fichier", base_path=str(base_path)
    )

    # --- Vérifications ---
    # Le dossier doit exister
    assert base_path.exists() and base_path.is_dir()

    # Le chemin doit se terminer par le bon répertoire
    assert str(base_path).endswith("app/data/data_clean/rapport_qualite")

    # Le fichier doit exister
    fichier = base_path / "test_fichier_rapport.txt"
    assert fichier.exists() and fichier.is_file()


def test_quality_rapport_contenu_fichier(
    expected_quality_report_fixture: pytest.fixture,
    expected_report_content: pytest.fixture,
    tmp_path: pytest.fixture,
):
    """
    Teste que le contenu du fichier rapport correspond aux données attendues.
    """
    base_path = tmp_path / "app/data/data_clean/rapport_qualite"

    # --- Exécution de la fonction ---
    quality_rapport(
        expected_quality_report_fixture, "test_fichier", base_path=str(base_path)
    )

    # --- Vérifications ---
    fichier = base_path / "test_fichier_rapport.txt"
    with open(fichier, "r") as f:
        contenu = f.read()

    # Vérifier que le contenu du fichier correspond aux données du rapport
    assert contenu == expected_report_content


# endregion

# region CREATE SILVER DF


def test_create_silver_df_report(
    expected_df_after_cleanup_fixture: pytest.fixture,
    expected_rapport_silver_df_fixture: pytest.fixture,
    transformations_fixture: pytest.fixture,
):
    """
    Teste que le rapport retourné par create_silver_df correspond au rapport attendu.
    """
    _, rapport = create_silver_df(
        expected_df_after_cleanup_fixture, transformations_fixture
    )

    assert rapport == expected_rapport_silver_df_fixture


@pytest.mark.parametrize(
    "score, duplicates_drop, partition_col, drop_cols, expected_df_name",
    [
        # Cas simple, pas de score, pas de déduplication, pas de partition, pas de drop
        (False, None, None, None, "expected_silver_df_fixture"),
        # Avec score (donc colonne 'score' en plus), pas de déduplication, pas de partition, pas de drop
        (True, None, None, None, "expected_silver_df_with_score_fixture"),
        # pas de score, partition_col, donc colonne date_partition en plus
        (
            False,
            False,
            "timestamp",
            None,
            "expected_silver_df_with_partition_fixture",
        ),
        # pas de score, déduplication, pas de partition, drop_cols: "fruit"
        (True, ["timestamp"], None, ["fruit","score"],  "expected_silver_df_with_dedup_and_drop_fixture"),
    ],
)
def test_create_silver_df_df(
    input_silver_df_fixture: pytest.fixture,
    transformations_fixture: pytest.fixture,
    score: bool,
    duplicates_drop: list | None,
    partition_col: list | None,
    drop_cols: list | None,
    expected_df_name: str,
    request: pytest.FixtureRequest,
):
    """
    Teste que le rapport retourné par create_silver_df correspond au rapport attendu.
    """
    expected_df_fixture = request.getfixturevalue(expected_df_name)
    result_df, _ = create_silver_df(
        input_silver_df_fixture,
        transformations_fixture,
        score=score,
        duplicates_drop=duplicates_drop,
        partition_col=partition_col,
        drop_cols=drop_cols,
    )

    result_sorted = result_df.orderBy("id")
    expected_sorted = expected_df_fixture.orderBy("id")

    assert result_sorted.schema == expected_sorted.schema
    assert result_sorted.collect() == expected_sorted.collect()


# endregion
