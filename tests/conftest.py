import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope ="session")

def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-pyspark")
        .getOrCreate()
    )

    yield  spark_session

    #cleanup
    spark_session.stop()
