import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")

# Local folder within the docker container
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# The name of the directory to store rides data within the GCS bucket
bucket_directory = "rides_data"

# URL of the bike usage dataset
dataset_url = "https://cycling.data.tfl.gov.uk/usage-stats"

def get_filenames_from_time(execution_date):
    """
    Each (weekly) file in the source bucket has a complex naming pattern including:
        - An ID incrementing with each week
        - The start date of the file, e.g. 04Jan2017
        - The end date of the file, e.g. 10Jan2017

    Here, we calculate these fields from the execution date

    :param execution_date: Date of scheduled airflow run. A datetime object is automatically passed to 
        this argument by airflow
    
    :return: The formatted file name for the airflow run's time period as a string.
    """

    # IDs start at 39 - this is added to the number of weeks since the first data chunk
    dataset_id = 38 + ((execution_date - datetime(2017, 1, 3, 20, tzinfo=execution_date.tzinfo)).days // 7)

    # Get the start and end ranges of the data (execution_date refers to the end of the data range)
    dataset_start_range = (execution_date - timedelta(days=6)).strftime("%d%b%Y")
    dataset_end_range = execution_date.strftime("%d%b%Y")

    # Get the properly formatted dataset name
    return f"{dataset_id}JourneyDataExtract{dataset_start_range}-{dataset_end_range}.csv"


with DAG(
    dag_id = "ingest_bike_locations",
    schedule_interval = "0 20 * * 2",
    catchup = True,
    max_active_runs = 3,
    tags = ["bike_locations"],
    start_date = datetime(2017, 1, 3, 20),
    end_date = datetime(2017, 1, 24, 20),  # datetime(2022, 1, 4, 20)
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0,
    }
) as ingest_bike_locations:

    # Get the name of the file using the date of the DAG run
    get_filenames = PythonOperator(
        task_id = "get_filenames",
        python_callable = get_filenames_from_time
    )

    # Get the return value of the get_filenames task
    dataset_name = "{{ ti.xcom_pull(task_ids='get_filenames') }}"
    logging.debug(f"dataset_name: {dataset_name}")

    # The week's data is downloaded to the local machine
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf {dataset_url}/{dataset_name} > {AIRFLOW_HOME}/{dataset_name}"
    )

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{dataset_name}",
        dst = f"{bucket_directory}/{dataset_name}",
        bucket = GCP_GCS_BUCKET,
        gzip = True
    )
    
    get_filenames >> download_file_from_https >> transfer_data_to_gcs
