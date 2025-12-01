# region IMPORTS
import pytest
from pyspark.sql import types as T
# endregion


# region DF ENTREE CLEAN DATE


@pytest.fixture
def clean_date_input_fixture(spark_session):
    data = [
        # (date_string, ligne_corrigee_init, ligne_invalide_init)
        ("2025-11-01T10:00:00", False, False),  # T remplacé par espace
        ("01/11/2025 10:00", False, False),  # / remplacé par -
        ("01.11.2025", False, False),  # . remplacé par -, reformattage DD-MM-YYYY
        ("01-11-2025 10:00:00", False, False),  # reformattage DD-MM-YYYY -> YYYY-MM-DD
        ("2025-11-01", False, False),  # pas d'heure, ajout "00:00:00"
        ("2025-11-01 10:00", False, False),  # HH:MM -> HH:MM:SS
        ("2025-11-01 10:00:00.123Z", False, False),  # tronqué à HH:MM:SS
        ("", False, False),  # vide -> invalide
        (None, False, False),  # None -> invalide
        ("not-a-date", False, False),  # string invalide mais renvoie tel quel
    ]

    schema = T.StructType(
        [
            T.StructField("date_string", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN DATE


@pytest.fixture
def clean_date_expected_fixture(spark_session):
    data = [
        # (value, ligne_corrigee, ligne_invalide)
        ("2025-11-01 10:00:00", True, False),  # T remplacé
        ("2025-11-01 10:00:00", True, False),  # / remplacé
        (
            "2025-11-01 00:00:00",
            True,
            False,
        ),  # . remplacé + reformatage date + ajout heure 00:00:00
        ("2025-11-01 10:00:00", True, False),  # reformatage DD-MM-YYYY -> YYYY-MM-DD
        ("2025-11-01 00:00:00", True, False),  # ajout heure 00:00:00
        ("2025-11-01 10:00:00", True, False),  # ajout secondes
        ("2025-11-01 10:00:00", True, False),  # tronqué à HH:MM:SS
        (None, False, True),  # vide -> invalide
        (None, False, True),  # None -> invalide
        (None, False, True),  # string non reconnue, renvoyée brute, pas invalidée
    ]

    schema = T.StructType(
        [
            T.StructField("value", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN TEMPERATURE


@pytest.fixture
def df_temperature_input_fixture(spark_session):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("temperature_string", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    data = [
        (1, "23", False, False),  # valide
        (2, "23,5", False, False),  # valide mais virgule → corrigé
        (3, "-25", False, False),  # hors bornes → invalide
        (4, "70", False, False),  # hors bornes → invalide
        (5, "abc", False, False),  # invalide (ValueError)
        (6, "", False, False),  # chaîne vide → invalide
        (7, None, False, False),  # None → invalide
    ]

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN TEMPERATURE


@pytest.fixture
def expected_temperature_output_fixture(spark_session):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("temperature_string", T.DoubleType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    data = [
        # input: "23", False, False
        (1, 23.0, False, False),
        # input: "23,5", False, False → virgule remplacée → ligne_corrigee=True
        (2, 23.5, True, False),
        # input: "-25" → hors bornes
        (3, None, False, True),
        # input: "70" → hors bornes
        (4, None, False, True),
        # input: "abc" → exception
        (5, None, False, True),
        # input: "" → invalide
        (6, None, False, True),
        # input: None → invalide
        (7, None, False, True),
    ]

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN RAIN MM


@pytest.fixture
def df_rain_input_fixture(spark_session):
    data = [
        (1, "12.5", False, False),  # Valide
        (2, "3,7", False, False),  # Virgule -> corrigée
        (3, "-1.2", False, False),  # Négatif -> invalide
        (4, "", False, False),  # Chaîne vide -> invalide
        (5, None, False, False),  # None -> invalide
        (6, "abc", False, False),  # Non convertible -> invalide
        (7, "0", False, False),  # Zéro -> valide
        (8, "25", False, False),  # Valide entier
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("rain_string", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN RAIN MM


@pytest.fixture
def expected_rain_output_fixture(spark_session):
    data = [
        (1, 12.5, False, False),  # "12.5"
        (2, 3.7, True, False),  # "3,7" -> corrigé
        (3, None, False, True),  # "-1.2" -> invalide
        (4, None, False, True),  # "" -> invalide
        (5, None, False, True),  # None -> invalide
        (6, None, False, True),  # "abc" -> invalide
        (7, 0.0, False, False),  # "0"
        (8, 25.0, False, False),  # "25"
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("rain_string", T.DoubleType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN WEATHER


@pytest.fixture
def df_weather_input_fixture(spark_session):
    data = [
        (1, "Rain", False, False),  # valide
        (2, "Cloudy", False, False),  # valide
        (3, "Clear", False, False),  # valide
        (4, "Drizzle", False, False),  # valide
        (5, "Fog", False, False),  # valide
        (6, "Sunny", False, False),  # invalide
        (7, "", False, False),  # vide -> invalide
        (8, None, False, False),  # None -> invalide
        (9, "rain", False, False),  # sensible à la casse, invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("weather_condition", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN WEATHER


@pytest.fixture
def expected_weather_output_fixture(spark_session):
    data = [
        (1, "Rain", False, False),
        (2, "Cloudy", False, False),
        (3, "Clear", False, False),
        (4, "Drizzle", False, False),
        (5, "Fog", False, False),
        (6, None, False, True),  # invalide
        (7, None, False, True),  # invalide
        (8, None, False, True),  # invalide
        (9, None, False, True),  # invalide (casse sensible)
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("weather_condition", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN LATITUDE


@pytest.fixture
def df_latitude_input_fixture(spark_session):
    data = [
        (1, "45.0", False, False),  # valide
        (2, "-90", False, False),  # limite valide
        (3, "90", False, False),  # limite valide
        (4, "91", False, False),  # invalide (>90)
        (5, "-91", False, False),  # invalide (<-90)
        (6, "", False, False),  # vide -> invalide
        (7, None, False, False),  # None -> invalide
        (8, "abc", False, False),  # non convertible -> invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("latitude", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN LATITUDE


@pytest.fixture
def expected_latitude_output_fixture(spark_session):
    data = [
        (1, 45.0, False, False),
        (2, -90.0, False, False),
        (3, 90.0, False, False),
        (4, None, False, True),  # invalide
        (5, None, False, True),  # invalide
        (6, None, False, True),  # invalide
        (7, None, False, True),  # invalide
        (8, None, False, True),  # invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("latitude", T.FloatType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN LONGITUDE


@pytest.fixture
def df_longitude_input_fixture(spark_session):
    data = [
        (1, "45.0", False, False),  # valide
        (2, "-180", False, False),  # limite valide
        (3, "180", False, False),  # limite valide
        (4, "181", False, False),  # invalide (>180)
        (5, "-181", False, False),  # invalide (<-180)
        (6, "", False, False),  # vide -> invalide
        (7, None, False, False),  # None -> invalide
        (8, "abc", False, False),  # non convertible -> invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("longitude", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN LONGITUDE


@pytest.fixture
def expected_longitude_output_fixture(spark_session):
    data = [
        (1, 45.0, False, False),
        (2, -180.0, False, False),
        (3, 180.0, False, False),
        (4, None, False, True),  # invalide
        (5, None, False, True),  # invalide
        (6, None, False, True),  # invalide
        (7, None, False, True),  # invalide
        (8, None, False, True),  # invalide
    ]

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("longitude", T.FloatType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )

    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN STATION NAME


@pytest.fixture
def df_station_name_input_fixture(spark_session):
    data = [
        (1, "Lille - Station 01", False, False),  # valide, pas corrigé
        (2, "Lille Station 01", False, False),  # invalide (manque le "-")
        (3, None, False, False),  # invalide (None)
        (4, "Paris - Station 01", False, False),  # invalide (autre ville)
        (5, "Lille - Station 123", False, False),  # invalide (numéro > 2 chiffres)
        (6, "", False, False),  # invalide (vide)
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_name", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN STATION NAME


@pytest.fixture
def df_station_name_expected_fixture(spark_session):
    data = [
        (1, "Lille - Station 01", False, False),  # validé, inchangé
        (2, None, False, True),  # invalide
        (3, None, False, True),  # invalide
        (4, None, False, True),  # invalide
        (5, None, False, True),  # invalide
        (6, None, False, True),  # invalide
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("station_name", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF ENTREE CLEAN POSITIVE INT


@pytest.fixture
def df_positive_int_input_fixture(spark_session):
    data = [
        (1, "123", False, False),
        (2, "0", False, False),
        (3, "-10", False, False),
        (4, None, False, False),
        (5, "abc", False, False),
        (6, "1", False, False),
        (7, "1.5", False, False),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("positive_int", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion

# region DF SORTIE CLEAN POSITIVE INT


@pytest.fixture
def expected_positive_int_output_fixture(spark_session):
    data = [
        (1, 123, True, False),
        (2, None, False, True),
        (3, None, False, True),
        (4, None, False, True),
        (5, None, False, True),
        (6, 1, True, False),
        (7, None, False, True),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("positive_int", T.IntegerType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
    return spark_session.createDataFrame(data, schema)


# endregion


@pytest.fixture
def df_nb_bikes_input_fixture(spark_session):
    data = [
        # id, velo_dispo_string, places_libres, capacity, ligne_corrigee, ligne_invalide
        (1, "5", "10", "20", False, False),      # valeur valide, entre 0 et capacité
        (2, "-1", "10", "20", False, False),     # velo_dispo_string négatif, corrigé à 0
        (3, "25", "10", "20", False, False),     # velo_dispo_string > capacity, corrigé à capacity
        (4, None, "10", "20", False, False),     # velo_dispo_string invalide, recalcul via capacity - places_libres
        (5, "abc", "10", "20", False, False),    # velo_dispo_string non numérique, recalcul valide
        (6, None, "15", "10", False, False),     # velo_dispo_string invalide, capacity < places_libres -> 0
        (7, None, None, "20", False, False),     # places_libres invalide, ligne invalide
        (8, None, "10", None, False, False),     # capacity invalide, ligne invalide
        (9, None, None, None, False, False),     # tout invalide
    ]
    schema = T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("nb_bikes", T.StringType()),
        T.StructField("places_libres", T.StringType()),
        T.StructField("capacity", T.StringType()),
        T.StructField("ligne_corrigee", T.BooleanType()),
        T.StructField("ligne_invalide", T.BooleanType()),
    ])
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_nb_bikes_output_fixture(spark_session):
    data = [
        # id, nb_bikes, ligne_corrigee, ligne_invalide
        (1, 5, False, False),
        (2, 10, True, False),
        (3, 20, True, False),
        (4, 10, True, False),
        (5, 10, True, False),
        (6, 0, True, False),
        (7, None, False, True),
        (8, None, False, True),
        (9, None, False, True),
    ]
    schema = T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("nb_bikes", T.IntegerType()),
        T.StructField("ligne_corrigee", T.BooleanType()),
        T.StructField("ligne_invalide", T.BooleanType()),
    ])
    return spark_session.createDataFrame(data, schema)