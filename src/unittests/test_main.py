import pytest
import json
import os
from datetime import datetime
from pyspark.testing.utils import assertDataFrameEqual
from unittest.mock import patch, PropertyMock
from google.cloud.dataproc_spark_connect import DataprocSparkSession

from src.main import sample_query


def load_json_data(filename):
    """Utility to load JSON data from the test_data directory."""
    filepath = os.path.join("test_data", filename)
    with open(filepath, "r") as f:
        data = json.load(f)

    for item in data:
        if "datehour" in item:
            item["datehour"] = datetime.fromisoformat(item["datehour"])
    return data


service_account_email = os.environ.get("UNIT_TEST_SERVICE_ACCOUNT")


@pytest.fixture(scope="session")
def spark_fixture():
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    spark = (
        DataprocSparkSession.builder.projectId("cf-data-analytics")
        .location("us-central1")
        .serviceAccount(service_account_email)
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_sample_query_mock_bigquery(spark_fixture):

    input_data = load_json_data("input_data.json")

    fake_df = spark_fixture.createDataFrame(input_data)

    with patch(
        "pyspark.sql.connect.session.SparkSession.read", new_callable=PropertyMock
    ) as mock_read_prop:
        mock_reader = mock_read_prop.return_value
        mock_reader.format.return_value.option.return_value.load.return_value = fake_df

        actual_df = sample_query(spark_fixture)

        mock_reader.format.assert_called_with("bigquery")
        mock_reader.format.return_value.option.assert_called_with(
            "table", "bigquery-public-data.wikipedia.pageviews_2024"
        )

    expected_data = load_json_data("expected_data.json")

    expected_df = spark_fixture.createDataFrame(expected_data).select(
        "title", "month", "total_views"
    )

    actual_df.show()
    expected_df.show()

    assertDataFrameEqual(actual_df, expected_df)
