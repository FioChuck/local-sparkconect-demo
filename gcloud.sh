gcloud dataproc batches submit pyspark src/main.py \
    --region="us-central1" \
    --version="2.3" \
    --deps-bucket="gs://cf-pyspark-dep" \
    -- \
    --gcs_path="gs://cf-wiki-data/wikipedia_output/"
