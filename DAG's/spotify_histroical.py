from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from google.api_core.retry import Retry


#  Definfing Default Arguments
PROJECT_ID = 'spotify-458017'
REGION = 'us-central1'  
CLUSTER_NAME = 'data-clean-cluster'
BUCKET_NAME = 'hsitoric_data'
GCS_SCRIPT_PATH = 'scripts/static_preprocessing.py'  
GCS_PROCESSED_FOLDER = 'cleandata/'       
DATASET_NAME = 'Top_songs'
TABLE_NAME = 'Historic_Table'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",  
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",  
    },
}

with DAG(
    'spotify_historic_etl_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

# Create Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
    )
#  Calling the Pyspark Job for Preprocessing
    preprocess_static_data = DataprocSubmitJobOperator(
        task_id="spark_job",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET_NAME}/{GCS_SCRIPT_PATH}",
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
    )
# Load the data to Bigquery
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[f"{GCS_PROCESSED_FOLDER}*"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format='PARQUET',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE'
    )
# Delete the Cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done", 
    )

    create_cluster >> preprocess_static_data >> load_to_bq >> delete_cluster

