# region IMPORTS
from pyspark.sql import functions as F, types as T
import re
# endregion


# region CLEAN DATE
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
    Fonction UDF pour nettoyer et formater une date donnée sous forme de chaîne de caractères.

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
        # etape 0 remplacer les "T" par des espaces
        if re.search(r"T", date_string):
            ligne_corrigee = True
            date_string = date_string.replace("T", " ")

        # etape 1 : remplacer les "/" et "." par "-"
        if re.search(r"[./]", date_string):
            ligne_corrigee = True
            date_string = re.sub(r"[./]", "-", date_string.strip())

        # etape 2 : couper la date et l'heure si besoin
        if " " in date_string:
            date_part = date_string.split(" ")[0]
            hour_part = date_string.split(" ")[1]
        else:
            date_part = date_string[:10]
            hour_part = date_string[10:]

        # etape 3 : reformater la date
        reformatted_date = None  # Initialiser la variable pour éviter l'erreur
        if re.match(r"^\d{2}-\d{2}-\d{4}$", date_part) or re.match(r"^\d{4}-\d{2}-\d{2}$", date_part):
            if re.match(r"^\d{2}-\d{2}-\d{4}$", date_part):
                reformatted_date = re.sub(
                    r"^(\d{2})-(\d{2})-(\d{4}$)", r"\3-\2-\1", date_part
                )
                ligne_corrigee = True
            else:
                reformatted_date = (
                    date_part  # Garder la date telle quelle si non modifiable
                )
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide

        # etape 4 : formater l'heure si besoin
        if len(hour_part.strip()) == 0:
            hour_part = "00:00:00"
            ligne_corrigee = True
        elif re.match(r"^\d{2}:\d{2}$", hour_part.strip()):
            hour_part = hour_part.strip() + ":00"
            ligne_corrigee = True
        elif re.match(r"^\d{2}:\d{2}:\d{2}.+", hour_part.strip()):
            hour_part = hour_part.strip()[:8]
            ligne_corrigee = True

        # etape 5 : combiner date et heure
        final_date_string = reformatted_date + " " + hour_part.strip()

        return final_date_string, ligne_corrigee, ligne_invalide

    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


# endregion

# region CLEAN TEMPERATURE


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
    Fonction UDF pour nettoyer et formater une température donnée sous forme de chaîne de caractères.

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


# endregion

# region CLEAN RAIN MM


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
    Fonction UDF pour nettoyer et formater une valeur de précipitations donnée sous forme de chaîne de caractères.

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


# endregion

# region CLEAN WEATHER


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_weather(
    weather_condition: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[str | None, bool, bool]:
    """
    Fonction UDF pour nettoyer et valider une condition météorologique donnée sous forme de chaîne de caractères.

    Args:
        weather_condition (str): La condition météorologique sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(float, bool, bool):
            - La condition météorologique validée (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (StringType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not weather_condition:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        if weather_condition in ["Rain", "Cloudy", "Clear", "Drizzle", "Fog"]:
            return weather_condition, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


# endregion

# region CLEAN LATITUDE


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.FloatType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_latitude(
    latitude: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[float | None, bool, bool]:
    """
    Fonction UDF pour nettoyer et valider une latitude donnée sous forme de chaîne de caractères.

    Args:
        latitude (str): La latitude sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(float | None, bool, bool):
            - La latitude validée (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (FloatType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not latitude:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        val = float(latitude)
        if val <= 90 and val >= -90:
            return val, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


# endregion

# region CLEAN LONGITUDE


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.FloatType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_longitude(
    longitude: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[float | None, bool, bool]:
    """
    Fonction UDF pour nettoyer et valider une longitude donnée sous forme de chaîne de caractères.

    Args:
        longitude (str): La longitude sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(float | None, bool, bool):
            - La longitude validée (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (FloatType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not longitude:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        val = float(longitude)
        if val <= 180 and val >= -180:
            return val, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide
    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


# endregion

# region CLEAN STATION NAME


@F.udf(
    T.StructType(
        [
            T.StructField("value", T.StringType()),
            T.StructField("ligne_corrigee", T.BooleanType()),
            T.StructField("ligne_invalide", T.BooleanType()),
        ]
    )
)
def clean_station_name(
    s: str, ligne_corrigee: bool, ligne_invalide: bool
) -> tuple[str | None, bool, bool]:
    """
    Fonction UDF pour nettoyer et valider un nom de station donné sous forme de chaîne de caractères.

    Args:
        s (str): Le nom de la station sous forme de chaîne de caractères.
        ligne_corrigee (bool): Indique si la ligne a été corrigée.
        ligne_invalide (bool): Indique si la ligne est invalide.

    Returns:
        tuple(str | None, bool, bool):
            - Le nom de la station validé (ou None si invalide).
            - Un booléen indiquant si la ligne a été corrigée.
            - Un booléen indiquant si la ligne est invalide.

        En Spark, le retour est représenté comme un STRUCT avec les champs :
            - value (StringType)
            - ligne_corrigee (BooleanType)
            - ligne_invalide (BooleanType)
    """
    if not s:
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide
    try:
        pattern = r"^Lille - Station \d{2}$"
        if re.fullmatch(pattern, s):
            return s, ligne_corrigee, ligne_invalide
        else:
            ligne_invalide = True
            return None, ligne_corrigee, ligne_invalide

    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


# endregion

# region CLEAN POSITIVE INT


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
    Fonction UDF pour nettoyer et valider un entier positif donné sous forme de chaîne de caractères.

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


# endregion

# region CLEAN NB BIKES


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
    Fonction UDF pour nettoyer et valider le nombre de vélos disponibles en fonction des données fournies.

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
    try:
        # Vérification des valeurs nulles ou non numériques pour places_libres et capacity
        places_libres_int = (
            places_libres
            if isinstance(places_libres, int)
            else int(places_libres)
            if isinstance(places_libres, str) and places_libres.isnumeric()
            else None
        )
        capacity_int = (
            int(capacity)
            if isinstance(capacity, str) and capacity.isnumeric()
            else None
        )

        if (not isinstance(velo_dispo_string, str)) or (
            not velo_dispo_string.isnumeric()
        ):
            if places_libres_int is not None and capacity_int is not None:
                diff = capacity_int - places_libres_int
                if diff >= 0:
                    ligne_corrigee = True
                    return diff, ligne_corrigee, ligne_invalide
                else:
                    ligne_corrigee = True
                    return 0, ligne_corrigee, ligne_invalide
            else:
                ligne_invalide = True
                return None, ligne_corrigee, ligne_invalide

        # Vérification de velo_dispo_string
        val = int(velo_dispo_string)
        if val < 0:
            ligne_corrigee = True
            return 0, ligne_corrigee, ligne_invalide
        elif capacity_int is not None and val > capacity_int:
            ligne_corrigee = True
            return capacity_int, ligne_corrigee, ligne_invalide
        else:
            return val, ligne_corrigee, ligne_invalide

    except (ValueError, TypeError):
        ligne_invalide = True
        return None, ligne_corrigee, ligne_invalide


# endregion
