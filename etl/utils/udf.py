from pyspark.sql import functions as F, types as T
import pandas as pd
import re


# pour des fonctions UDF qui retournent plusieurs valeurs (multi-colonnes)
@F.udf(
    T.StructType(
        [
            T.StructField("value", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_date(
    date_string: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[str, bool, bool]:
    """
    Nettoie et formate une date donnée sous forme de chaîne de caractères.

    Args:
        date_string (str): La date sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(str, bool, bool):
            - La date formatée (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (StringType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not date_string:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        d = pd.to_datetime(date_string, errors="coerce")
        if pd.isnull(d):
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
        formated_date = d.strftime("%Y-%m-%d %H:%M:%S")
        if formated_date != date_string:
            ligne_corrigee = True
        return formated_date, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.DoubleType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_temperature(
    temperature_string: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[float, bool, bool]:
    """
    Nettoie et formate une température donnée sous forme de chaîne de caractères.

    Args:
        temperature_string (str): La température sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(float, bool, bool):
            - La température formatée (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (DoubleType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not temperature_string:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        formated_line = str(temperature_string).replace(",", ".")
        val = float(formated_line)
        if val >= -20 and val <= 50:
            if formated_line != temperature_string:
                ligne_corrigee = True
            return val, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.DoubleType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_rain_mm(
    rain_string: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[float, bool, bool]:
    """
    Nettoie et formate une valeur de précipitations donnée sous forme de chaîne de caractères.

    Args:
        rain_string (str): La valeur des précipitations sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(float, bool, bool):
            - La valeur des précipitations formatée (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (DoubleType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not rain_string:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        formated_line = str(rain_string).replace(",", ".")
        val = float(formated_line)
        if val >= 0:
            if formated_line != rain_string:
                ligne_corrigee = True
            return val, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_weather(s: str, ligne_corrigee: bool, ligne_invalide: bool) -> tuple[str | None, bool, bool]:
    """
    Nettoie et valide une condition météorologique donnée sous forme de chaîne de caractères.

    Args:
        s (str): La condition météorologique sous forme de chaîne de caractères.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(float, bool):
            - La condition météorologique validée (ou None si invalide).
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (StringType)
            - ligne_invalide (BooleanType)
    """
    if not s:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        if s in ["Rain", "Cloudy", "Clear", "Drizzle", "Fog"]:
            return s, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


@F.udf(T.FloatType())
def clean_latitude(s: str) -> float | None:
    """
    Nettoie et valide une latitude donnée sous forme de chaîne de caractères.

    Args:
        s (str): La latitude sous forme de chaîne de caractères.

    Returns:
        float | None: La latitude validée (ou None si invalide).

        En Spark, le retour est représenté comme un champ FloatType.
    """
    if not s:
        return None
    try:
        val = float(s)
        return val if val <= 90 and val >= -90 else None
    except (ValueError, TypeError):
        return None


@F.udf(T.FloatType())
def clean_longitude(s: str) -> float | None:
    """
    Nettoie et valide une longitude donnée sous forme de chaîne de caractères.

    Args:
        s (str): La longitude sous forme de chaîne de caractères.

    Returns:
        float | None: La longitude validée (ou None si invalide).

        En Spark, le retour est représenté comme un champ FloatType.
    """
    if not s:
        return None
    try:
        val = float(s)
        return val if val <= 180 and val >= -180 else None
    except (ValueError, TypeError):
        return None


@F.udf(T.StringType())
def clean_station_name(s: str) -> str | None:
    """
    Nettoie et valide un nom de station donné sous forme de chaîne de caractères.

    Args:
        s (str): Le nom de la station sous forme de chaîne de caractères.

    Returns:
        str | None: Le nom de la station validé (ou None si invalide).

        En Spark, le retour est représenté comme un champ StringType.
    """
    if not s:
        return None
    try:
        pattern = r"^Lille - Station \d{2}$"
        if re.fullmatch(pattern, s):
            return s
        else:
            return None

    except (ValueError, TypeError):
        return None


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.IntegerType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_positive_int(
    positive_int_string: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[int, bool, bool]:
    """
    Nettoie et valide un entier positif donné sous forme de chaîne de caractères.

    Args:
        positive_int_string (str): L'entier positif sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(int, bool, bool):
            - L'entier positif validé (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (IntegerType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not positive_int_string:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        val = int(positive_int_string)
        if val > 0:
            ligne_corrigee = True
            return val, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.IntegerType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_nb_bikes(
    velo_dispo_string: str,
    places_libres: str,
    capacity: str,
    ligne_corrigee: bool,
    ligne_invalide: bool,
) -> tuple[int, bool, bool]:
    """
    Nettoie et valide le nombre de vélos disponibles en fonction des données fournies.

    Args:
        velo_dispo_string (str): Le nombre de vélos disponibles sous forme de chaîne de caractères.
        places_libres (str): Le nombre de places libres sous forme de chaîne de caractères.
        capacity (str): La capacité totale sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(int, bool, bool):
            - Le nombre de vélos disponibles validé (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (IntegerType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not velo_dispo_string or not velo_dispo_string.isnumeric():
        if places_libres and capacity:
            try:
                diff = int(capacity) - int(places_libres)
                if diff >= 0:
                    ligne_corrigee = True
                    return diff, ligne_corrigee, ligne_invalide
                else:
                    ligne_corrigee = True
                    return 0, ligne_corrigee, ligne_invalide
            except (ValueError, TypeError):
                ligne_invalide = True
                return None, ligne_corrigee, ligne_invalide

    try:
        val = int(velo_dispo_string)
        if val < 0:
            ligne_corrigee = True
            return 0, ligne_corrigee, ligne_invalide
        elif val > int(capacity):
            ligne_corrigee = True
            return int(capacity), ligne_corrigee, ligne_invalide
        else:
            return val, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
