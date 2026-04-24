import pytest
from datetime import datetime
from pyspark.testing.utils import assertDataFrameEqual

# Import the Dataproc Spark Connect session builder
from google.cloud.dataproc_spark_connect import DataprocSparkSession

from main import transform_pageviews

@pytest.fixture(scope="session")
def spark_fixture():
    """
    Creates a Dataproc Spark Connect Session for testing.
    This routes all PySpark commands to your GCP Dataproc environment.
    """
    
    spark = (
        DataprocSparkSession.builder
        .projectId('cf-data-analytics')
        .location('us-central1')
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()

def test_transform_pageviews(spark_fixture):
    """
    Tests the pageview transformation logic using mock data over Spark Connect.
    """
    # 1. Define input data covering expected conditions and edge cases
    input_data = [
        {"datehour": datetime(2024, 1, 1, 10, 0, 0), "wiki": "en", "title": "Python", "views": 100},
        {"datehour": datetime(2024, 1, 1, 11, 0, 0), "wiki": "en", "title": "Python", "views": 50},
        {"datehour": datetime(2024, 1, 1, 12, 0, 0), "wiki": "en", "title": "PySpark", "views": 200},
        {"datehour": datetime(2024, 1, 1, 13, 0, 0), "wiki": "fr", "title": "Python", "views": 500}, # Should be filtered out
        {"datehour": datetime(2024, 1, 2, 10, 0, 0), "wiki": "en", "title": "Python", "views": 300}, # Should be filtered out
    ]
    
    # This automatically sends the local Python data to your Dataproc cluster
    input_df = spark_fixture.createDataFrame(input_data)
    
    # 2. Run the transformation function remotely
    actual_df = transform_pageviews(input_df)
    
    # 3. Define the expected output data 
    expected_data = [
        {"title": "PySpark", "month": "January", "total_views": 200},
        {"title": "Python", "month": "January", "total_views": 150},
    ]
    
    # Force the column order to exactly match the output of transform_pageviews
    expected_df = spark_fixture.createDataFrame(expected_data).select(
        "title", "month", "total_views"
    )
    
    # 4. Assert equality 
    # PySpark 3.5+ supports assertDataFrameEqual over Spark Connect natively
    assertDataFrameEqual(actual_df, expected_df)