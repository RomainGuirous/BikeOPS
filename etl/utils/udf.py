from pyspark.sql import functions as F, types as T
import pandas as pd
import re


@F.udf(T.StringType())
def clean_date(s: str, lignes_corrigees: str, valeurs_invalides: str) -> str | None:
    """
    Nettoie une chaîne de caractères pour obtenir une date au format YYYY-MM-DD HH:MM:SS.

    Args:
        s (str): La chaîne de caractères à nettoyer.
        lignes_corrigees (str): Accumulateur pour le nombre de lignes corrigées.
        valeurs_invalides (str): Accumulateur pour le nombre de valeurs invalides.

    Returns:
        str or None: La date formatée si la conversion est réussie, sinon None.
    """
    if not s:
        valeurs_invalides += 1
        return None
    try:
        d = pd.to_datetime(s, errors="coerce")
        if pd.isnull(d):
            valeurs_invalides += 1
            return None
        formated_date = d.strftime("%Y-%m-%d %H:%M:%S")
        if formated_date != s:
            lignes_corrigees += 1
        return formated_date
    except (ValueError, TypeError):
        valeurs_invalides += 1
        return None


@F.udf(T.DoubleType())
def clean_temperature(
    s: str, lignes_corrigees: str, valeurs_invalides: str
) -> float | None:
    """
    Nettoie une chaîne de caractères pour obtenir une température en degrés Celsius.

    Args:
        s (str): La chaîne de caractères à nettoyer.
        lignes_corrigees (str): Accumulateur pour le nombre de lignes corrigées.
        valeurs_invalides (str): Accumulateur pour le nombre de valeurs invalides.

    Returns:
        float or None: La température en degrés Celsius si la conversion est réussie et que la température est dans un intervalle réaliste, sinon None.
    """
    if not s:
        valeurs_invalides += 1
        return None
    try:
        formated_line = str(s).replace(",", ".")
        val = float(formated_line)
        if val >= -20 and val <= 50:
            if formated_line != s:
                lignes_corrigees += 1
            return val
        else:
            valeurs_invalides += 1
            return None
    except (ValueError, TypeError):
        valeurs_invalides += 1
        return None


@F.udf(T.DoubleType())
def clean_rain_mm(
    s: str, lignes_corrigees: str, valeurs_invalides: str
) -> float | None:
    """
    Nettoie une chaîne de caractères pour obtenir une quantité de pluie en millimètres.

    Args:
        s (str): La chaîne de caractères à nettoyer.
        lignes_corrigees (str): Accumulateur pour le nombre de lignes corrigées.
        valeurs_invalides (str): Accumulateur pour le nombre de valeurs invalides.

    Returns:
        float or None: La quantité de pluie en millimètres si la conversion est réussie et que la valeur est positive, sinon None.
    """
    if not s:
        return None
    try:
        formated_line = str(s).replace(",", ".")
        val = float(formated_line)
        if val >= 0:
            if formated_line != s:
                lignes_corrigees += 1
            return val
        else:
            valeurs_invalides += 1
            return None
    except (ValueError, TypeError):
        valeurs_invalides += 1
        return None


@F.udf(T.StringType())
def clean_weather(s: str, valeurs_invalides: str) -> str | None:
    """
    Nettoie une chaîne de caractères pour obtenir une condition météorologique.

    Args:
        s (str): La chaîne de caractères à nettoyer.
        valeurs_invalides (str): Accumulateur pour le nombre de valeurs invalides.

    Returns:
        str or None: La condition météorologique si la valeur est valide, sinon None.
    """
    if not s:
        valeurs_invalides += 1
        return None
    try:
        if s in ["Rain", "Cloudy", "Clear", "Drizzle", "Fog"]:
            return s
        else:
            valeurs_invalides += 1
            return None
    except (ValueError, TypeError):
        valeurs_invalides += 1
        return None


@F.udf(T.FloatType())
def clean_latitude(s: str) -> float | None:
    if not s:
        return None
    try:
        val = float(s)
        return val if val <= 90 and val >= -90 else None
    except (ValueError, TypeError):
        return None


@F.udf(T.FloatType())
def clean_longitude(s: str) -> float | None:
    if not s:
        return None
    try:
        val = float(s)
        return val if val <= 180 and val >= -180 else None
    except (ValueError, TypeError):
        return None


@F.udf(T.StringType())
def clean_station_name(s: str) -> str | None:
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


@F.udf(T.IntegerType())
def clean_positive_int(
    s: str, lignes_corrigees: str, valeurs_invalides: str
) -> int | None:
    """
    Nettoie une chaîne de caractères pour obtenir un entier positif.

    Args:
        s (str): La chaîne de caractères à nettoyer.
        lignes_corrigees (str): Accumulateur pour le nombre de lignes corrigées.
        valeurs_invalides (str): Accumulateur pour le nombre de valeurs invalides.

    Returns:
        int or None: L'entier positif si la conversion est réussie et que l'entier est positif, sinon None.
    """
    if not s:
        valeurs_invalides += 1
        return None
    try:
        val = int(s)
        if val > 0:
            if val != s:
                lignes_corrigees += 1
            return val
        else:
            valeurs_invalides += 1
            return None
    # on attend ValueError ou TypeError, on attrape les deux pour éviter de masquer d'autres erreurs
    except (ValueError, TypeError):
        valeurs_invalides += 1
        return None


@F.udf(T.IntegerType())
def clean_nb_bikes(
    velo_dispo: str,
    places_libres: str,
    capacity: str,
    lignes_corrigees: str,
    valeurs_invalides: str,
) -> int | None:
    """
    Nettoie une chaîne de caractères pour obtenir un nombre de vélos disponibles.

    Args:
        velo_dispo (str): La chaîne de caractères représentant le nombre de vélos disponibles.
        places_libres (str): La chaîne de caractères représentant le nombre de places libres.
        capacity (str): La chaîne de caractères représentant la capacité totale de la station.
        lignes_corrigees (str): Accumulateur pour le nombre de lignes corrigées.
        valeurs_invalides (str): Accumulateur pour le nombre de valeurs invalides.

    Returns:
        int or None: Le nombre de vélos disponibles si la conversion est réussie et que la valeur est dans un intervalle réaliste, sinon None.
    """
    # si velo_dispo Null ou non numeric, on essaie de le créer avec les valeurs capacity et places_libres, sinon un None
    if not velo_dispo or not velo_dispo.isnumeric():
        if places_libres and capacity:
            try:
                diff = int(capacity) - int(places_libres)
                if diff >= 0:
                    lignes_corrigees += 1
                    return diff
                else:
                    lignes_corrigees += 1
                    return 0
            except (ValueError, TypeError):
                valeurs_invalides += 1
                return None

    try:
        val = int(velo_dispo)
        if val < 0:
            lignes_corrigees += 1
            return 0
        elif val > int(capacity):
            return int(capacity)
        else:
            return val
    except (ValueError, TypeError):
        valeurs_invalides += 1
        return None
