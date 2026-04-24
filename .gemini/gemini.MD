# Role
You are an expert PySpark data engineer. 

# Architecture & Deployment
This project is designed to be deployed as a batch job on Google Cloud Dataproc Serverless. 
All PySpark boilerplate must support both Dataproc Serverless deployment and local remote debugging via Dataproc Spark Connect.

# Coding Standards
- **Spark Session Initialization:** Always use `argparse` to provide a `--local_connect` flag.
- **Local Connect:** If `--local_connect` is true, import `DataprocSparkSession` from `google.cloud.dataproc_spark_connect`. The Spark session must be configured to use a service account for authentication to avoid "end user credentials" errors. This is achieved by:
    - Ensuring the `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to your service account key file.
    - Setting `GOOGLE_CLOUD_PROJECT` and `GOOGLE_CLOUD_REGION` environment variables.
    - Setting the `DATAPROC_SERVICE_ACCOUNT` environment variable with the full service account email (e.g., `local-dev@<project-id>.iam.gserviceaccount.com`).
    - The `DataprocSparkSession.builder` should then use these environment variables via `.projectId(os.environ.get("GOOGLE_CLOUD_PROJECT"))`, `.location(os.environ.get("GOOGLE_CLOUD_REGION"))`, and `.serviceAccount(os.environ.get("DATAPROC_SERVICE_ACCOUNT"))`.
    - DO NOT provide a `dataprocSessionId`. Allow Dataproc to auto-provision a new session.
    - When executing Python scripts locally within this project, the Gemini CLI MUST always append the `--local_connect` flag to the execution command.
- **Standard Execution:** If `--local_connect` is false, fallback to the standard `SparkSession.builder`.
- **Modularity:** Isolate core DataFrame transformations into easily testable functions that take and return DataFrames.