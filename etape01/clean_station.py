from pyspark.sql import SparkSession, functions as F, types as T
import pandas as pd
import os

@F.udf(T.IntegerType())
def clean_positive_int(s: str) -> int | None:
    """
    Nettoie une chaîne de caractères pour obtenir un entier positif.
    
    Args:
        s (str): La chaîne de caractères à nettoyer.
        
    Returns:
        int or None: L'entier positif si la conversion est réussie et que l'entier est positif, sinon None.
    """
    if not s:
        return None
    try:
        val = int(s)
        return val if val > 0 else None
    # on attend ValueError ou TypeError, on attrape les deux pour éviter de masquer d'autres erreurs
    except (ValueError, TypeError):
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