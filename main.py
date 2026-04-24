import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, sum

def transform_pageviews(df: DataFrame) -> DataFrame:
    """Core transformation logic isolated for unit testing."""
    return (
        df.filter(col("datehour").cast("date") == "2024-01-01")
       .filter(col("wiki") == "en")
       .withColumn("month", date_format(col("datehour"), "MMMM"))
       .groupBy("title", "month")
       .agg(sum("views").alias("total_views"))
       .orderBy(col("total_views").desc())
       .limit(10000)
    )

def sample_query(spark: SparkSession, gcs_path: str):
    bq_table = "bigquery-public-data.wikipedia.pageviews_2024"

    raw_df = spark.read.format("bigquery").option("table", bq_table).load()
    
    aggregated_df = transform_pageviews(raw_df)
    aggregated_df.show()

    aggregated_df.write.format("parquet").mode("overwrite").partitionBy("month").save(gcs_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo PySpark Job")
    parser.add_argument("--gcs_path", required=True, help="Destination GCS path for output")
    
    parser.add_argument(
        "--local_connect", 
        action="store_true", 
        help="Use Dataproc Spark Connect for local debugging"
    )
    
    args = parser.parse_args()

    if args.local_connect:
        from google.cloud.dataproc_spark_connect import DataprocSparkSession
        import os
        
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        location = os.environ.get("GOOGLE_CLOUD_REGION")
        service_account_email = os.environ.get("DATAPROC_SERVICE_ACCOUNT")
        
        spark = (
            DataprocSparkSession.builder
            .projectId(project_id)
            .location(location)
            .serviceAccount(service_account_email)
            .getOrCreate()
        )
        
    else:
        spark = SparkSession.builder.appName("Wikipedia_Pageviews_Job").getOrCreate()

    try:
        sample_query(spark, args.gcs_path)
    finally:

        print("test")
        spark.stop()