# # unittests/fixtures/fixtures.py
# import pytest
# from pyspark.sql import SparkSession
# from typing import Generator, Any

# @pytest.fixture(scope='session')
# def test_spark() -> Generator[SparkSession, Any, Any]:
#   spark = (
#    SparkSession.\
#     builder.\
#     master('local[*]').\
#     appName('test_accounting_app').\
#     getOrCreate() #type: ignore
#   )
#   yield spark
#   spark.stop()

# class FixtureBase:
#   def __init__(self, test_spark: SparkSession):
#     self.test_spark = test_spark
