from pyspark.sql import SparkSession
import pytest
from tests.common.fixtures.fixture_spark_functions import *
from tests.common.fixtures.fixture_udf import *


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