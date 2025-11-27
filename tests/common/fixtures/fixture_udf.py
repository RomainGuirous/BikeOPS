# region IMPORTS
import pytest
from pyspark.sql import functions as F, types as T
# endregion


# region DF ENTREE CLEAN DATE


@pytest.fixture
def clean_date_input_fixture(spark_session):
    data = [
        # (date_string, ligne_corrigee_init, ligne_invalide_init)

        ("2025-11-01T10:00:00", False, False),   # T remplacé par espace
        ("01/11/2025 10:00", False, False),      # / remplacé par -
        ("01.11.2025", False, False),            # . remplacé par -, reformattage DD-MM-YYYY
        ("01-11-2025 10:00:00", False, False),   # reformattage DD-MM-YYYY -> YYYY-MM-DD
        ("2025-11-01", False, False),            # pas d'heure, ajout "00:00:00"
        ("2025-11-01 10:00", False, False),      # HH:MM -> HH:MM:SS
        ("2025-11-01 10:00:00.123Z", False, False),  # tronqué à HH:MM:SS

        ("", False, False),                      # vide -> invalide
        (None, False, False),                    # None -> invalide
        ("not-a-date", False, False),            # string invalide mais renvoie tel quel
    ]

    schema = T.StructType([
        T.StructField("date_string", T.StringType()),
        T.StructField("ligne_corrigee", T.BooleanType()),
        T.StructField("ligne_invalide", T.BooleanType()),
    ])

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN DATE

@pytest.fixture
def clean_date_expected_fixture(spark_session):
    data = [
        # (value, ligne_corrigee, ligne_invalide)

        ("2025-11-01 10:00:00", True, False),    # T remplacé
        ("2025-11-01 10:00:00", True, False),    # / remplacé
        ("2025-11-01 00:00:00", True, False),    # . remplacé + reformatage date + ajout heure 00:00:00
        ("2025-11-01 10:00:00", True, False),    # reformatage DD-MM-YYYY -> YYYY-MM-DD
        ("2025-11-01 00:00:00", True, False),    # ajout heure 00:00:00
        ("2025-11-01 10:00:00", True, False),    # ajout secondes
        ("2025-11-01 10:00:00", True, False),    # tronqué à HH:MM:SS

        (None, False, True),                      # vide -> invalide
        (None, False, True),                      # None -> invalide
        (None, False, True),             # string non reconnue, renvoyée brute, pas invalidée
    ]

    schema = T.StructType([
        T.StructField("value", T.StringType()),
        T.StructField("ligne_corrigee", T.BooleanType()),
        T.StructField("ligne_invalide", T.BooleanType()),
    ])

    return spark_session.createDataFrame(data, schema)


# endregion