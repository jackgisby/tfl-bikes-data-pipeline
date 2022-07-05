import logging
from os import environ
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
                                                              DataprocSubmitPySparkJobOperator, \
                                                              DataprocDeleteClusterOperator


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")
GCP_PROJECT_DATAPROC_CLUSTER_NAME = environ.get("GCP_PROJECT_DATAPROC_CLUSTER_NAME", "bikes-cluster")
GCS_REGION = environ.get("GCS_REGION", "europe-north1")
GCS_REGION = "europe-west2"

# Local folder within the docker container
AIRFLOW_HOME = environ.get("AIRFLOW_HOME", "/opt/airflow/")
SPARK_HOME = environ.get("SPARK_HOME", "/opt/spark/")

# Whether to setup/deconstruct cluster resources or assume they are already present
CREATE_INFRASTRUCTURE = bool(environ.get("CREATE_INFRASTRUCTURE", True))


def get_previous_month_as_yyyymm(year, month):
    """
    Wrapper to the function `get_previous_month` that gets the output as a 
    string in the format "YYYYMM".

    :param year: A string representing the year (YYYY)

    :param month: A string representing the month (MM)

    :return: A string in the format "YYYYMM" with the date of the previous month.
    """

    from ingest_weather_data import get_previous_month

    year, month, _ = get_previous_month(year, month)

    return f"{year}{month}"

def get_cluster_setup_task():
    "Creates a lightweight `DataprocCreateClusterOperator` task based on GCP variables."

    return DataprocCreateClusterOperator(
        task_id = "create_cluster",
        project_id = GCP_PROJECT_ID,
        cluster_name = GCP_PROJECT_DATAPROC_CLUSTER_NAME,
        region = GCS_REGION,
        cluster_config = {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
            },
        }
    )

def get_cluster_job_submission_task(file_name, additional_arguments=None):
    """
    Submits a spark job to an active cluster on GCP.
    
    :param file_name: The name of the file within the bucket specified by `GCP_GCS_BUCKET`
        to be run by the job.

    :param additional_arguments: If specified, these arguments will be given to
        the spark script in addition to the project ID, bucket ID and BigQuery
        dataset ID.

    :return: A `DataprocSubmitPySparkJobOperator` task.
    """

    job_arguments = [GCP_PROJECT_ID, GCP_GCS_BUCKET, BIGQUERY_DATASET]

    if additional_arguments:
        job_arguments.append(additional_arguments)

    return DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_spark_job_task",
        main = f"gs://{GCP_GCS_BUCKET}/spark/{file_name}.py",
        arguments = additional_arguments,
        cluster_name = GCP_PROJECT_DATAPROC_CLUSTER_NAME,
        region = GCS_REGION,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"]
    )

def get_cluster_teardown_task():
    "Destroys a cluster using a `DataprocDeleteClusterOperator` task."

    return DataprocDeleteClusterOperator(
        task_id = "delete_cluster",
        project_id = GCP_PROJECT_ID,
        cluster_name = GCP_PROJECT_DATAPROC_CLUSTER_NAME,
        region = GCS_REGION,
        trigger_rule = "all_done",
    )

def get_pyspark_upload_task(file_name):
    """
    Moves a pyspark file to GCP in order to be run on a cluster instance.

    :param file_name: The file name of the pyspark file to be uploaded.

    :return: A `LocalFilesystemToGCSOperator` task.
    """

    return LocalFilesystemToGCSOperator(
        task_id = "upload_pyspark_file",
        src = f"{SPARK_HOME}/{file_name}",
        dst = f"spark/{file_name}",
        bucket = GCP_GCS_BUCKET
    )

with DAG(
    dag_id = "setup_bigquery",
    schedule_interval = "@once",  # One-time setup
    catchup = False,
    max_active_runs = 1,
    tags = ["create_warehouse"],
    start_date = days_ago(1),
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0,
    }
) as setup_bigquery:
    
    upload_pyspark_file = get_pyspark_upload_task("setup_database.py")

    if CREATE_INFRASTRUCTURE:
        create_cluster = get_cluster_teardown_task()

    submit_dataproc_spark_job_task = get_cluster_job_submission_task("setup_database.py")
    
    if CREATE_INFRASTRUCTURE:
        delete_cluster = get_cluster_teardown_task()

    upload_pyspark_file >> submit_dataproc_spark_job_task

    if CREATE_INFRASTRUCTURE:
        create_cluster >> submit_dataproc_spark_job_task >> delete_cluster

with DAG(
    dag_id = "spark_transform_load",
    schedule_interval = "0 0 10 * *",  # Transform/load previous month's data on the 10th
    catchup = True,
    max_active_runs = 1,
    tags = ["update_warehouse"],
    start_date = datetime(2017, 1, 20),
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    }
) as spark_transform_load:

    upload_pyspark_file = get_pyspark_upload_task("transform_load.py")

    # Get the month (in YYYYMM format) of the current DAG run
    get_previous_month = PythonOperator(
        task_id = "get_previous_month",
        python_callable = get_previous_month_as_yyyymm,
        op_kwargs = {
            "year": "{{ execution_date.strftime('%Y') }}", 
            "month": "{{ execution_date.strftime('%m') }}"
        }
    )

    yyyymm = "{{ ti.xcom_pull(task_ids='get_previous_month') }}"
    logging.info(f"YYYYMM: {yyyymm}")

    if CREATE_INFRASTRUCTURE:
        create_cluster = get_cluster_setup_task()

    # Submit job with an additional argument: The year and month ("YYYYMM") of the data
    submit_dataproc_spark_job_task = get_cluster_job_submission_task(
        "transform_load.py", 
        additional_arguments = [yyyymm]
    )

    if CREATE_INFRASTRUCTURE:
        delete_cluster = get_cluster_teardown_task()

    upload_pyspark_file >> submit_dataproc_spark_job_task
    get_previous_month >> submit_dataproc_spark_job_task
    
    if CREATE_INFRASTRUCTURE:
        create_cluster >> submit_dataproc_spark_job_task >> delete_cluster
