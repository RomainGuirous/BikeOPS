from pyspark.sql import functions as F, types as T
import pandas as pd
import re


def clean_date_udf(acc_corrigees, acc_invalides):
    def clean_date(s):
        if not s:
            acc_invalides.add(1)
            return None
        try:
            d = pd.to_datetime(s, errors="coerce")
            if pd.isnull(d):
                acc_invalides.add(1)
                return None
            formated_date = d.strftime("%Y-%m-%d %H:%M:%S")
            if formated_date != s:
                acc_corrigees.add(1)
            return formated_date
        except (ValueError, TypeError):
            acc_invalides.add(1)
            return None

    return F.udf(clean_date, T.StringType())


def clean_temperature_udf(acc_corrigees, acc_invalides):
    def clean_temperature(s):
        if not s:
            acc_invalides.add(1)
            return None
        try:
            formated_line = str(s).replace(",", ".")
            val = float(formated_line)
            if val >= -20 and val <= 50:
                if formated_line != s:
                    acc_corrigees.add(1)
                return val
            else:
                acc_invalides.add(1)
                return None
        except (ValueError, TypeError):
            acc_invalides.add(1)
            return None

    return F.udf(clean_temperature, T.DoubleType())


def clean_rain_mm_udf(acc_corrigees, acc_invalides):
    def clean_rain_mm(s):
        if not s:
            acc_invalides.add(1)
            return None
        try:
            formated_line = str(s).replace(",", ".")
            val = float(formated_line)
            if val >= 0:
                if formated_line != s:
                    acc_corrigees.add(1)
                return val
            else:
                acc_invalides.add(1)
                return None
        except (ValueError, TypeError):
            acc_invalides.add(1)
            return None

    return F.udf(clean_rain_mm, T.DoubleType())


def clean_weather_udf(acc_invalides):
    def clean_weather(s):
        if not s:
            acc_invalides.add(1)
            return None
        try:
            if s in ["Rain", "Cloudy", "Clear", "Drizzle", "Fog"]:
                return s
            else:
                acc_invalides.add(1)
                return None
        except (ValueError, TypeError):
            acc_invalides.add(1)
            return None

    return F.udf(clean_weather, T.StringType())


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


def clean_positive_int_udf(acc_corrigees, acc_invalides):
    def clean_positive_int(s):
        if not s:
            acc_invalides.add(1)
            return None
        try:
            val = int(s)
            if val > 0:
                acc_corrigees.add(1)
                return val
            else:
                acc_invalides.add(1)
                return None
        except (ValueError, TypeError):
            acc_invalides.add(1)
            return None

    return F.udf(clean_positive_int, T.IntegerType())


def clean_nb_bikes_udf(acc_corrigees, acc_invalides):
    def clean_nb_bikes(velo_dispo, places_libres, capacity):
        if not velo_dispo or not velo_dispo.isnumeric():
            if places_libres and capacity:
                try:
                    diff = int(capacity) - int(places_libres)
                    if diff >= 0:
                        acc_corrigees.add(1)
                        return diff
                    else:
                        acc_corrigees.add(1)
                        return 0
                except (ValueError, TypeError):
                    acc_invalides.add(1)
                    return None

        try:
            val = int(velo_dispo)
            if val < 0:
                acc_corrigees.add(1)
                return 0
            elif val > int(capacity):
                acc_corrigees.add(1)
                return int(capacity)
            else:
                return val
        except (ValueError, TypeError):
            acc_invalides.add(1)
            return None

    return F.udf(clean_nb_bikes, T.IntegerType())
