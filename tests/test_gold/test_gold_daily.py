# region IMPORTS
import pytest
from pyspark.sql import functions as F
from etl.gold.gold_daily import (
    dim_station,
    dim_weather,
    dim_date,
)
# endregion
