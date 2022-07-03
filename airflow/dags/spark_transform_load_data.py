from os import environ
from datetime import datetime

from airflow import DAG
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

with DAG(
    dag_id = "spark_transform_load_to_bigquery",
    schedule_interval = "0 0 20 * *",
    catchup = False,
    max_active_runs = 1,
    tags = ["create_warehouse"],
    start_date = datetime(2017, 1, 20),
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    }
) as spark_transform_load_to_bigquery:

    upload_pyspark_file = LocalFilesystemToGCSOperator(
        task_id = "upload_pyspark_file",
        src = f"{SPARK_HOME}/transform_load_data.py",
        dst = f"spark/transform_load_data.py",
        bucket = GCP_GCS_BUCKET
    )

    create_cluster = DataprocCreateClusterOperator(
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

    submit_dataproc_spark_job_task = DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_spark_job_task",
        main = f"gs://{GCP_GCS_BUCKET}/spark/transform_load_data.py",
        arguments = [GCP_PROJECT_ID, GCP_GCS_BUCKET, BIGQUERY_DATASET],
        cluster_name = GCP_PROJECT_DATAPROC_CLUSTER_NAME,
        region = GCS_REGION,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"]
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id = "delete_cluster",
        project_id = GCP_PROJECT_ID,
        cluster_name = GCP_PROJECT_DATAPROC_CLUSTER_NAME,
        region = GCS_REGION,
        trigger_rule = "all_done",
    )

    upload_pyspark_file >> submit_dataproc_spark_job_task
    create_cluster >> submit_dataproc_spark_job_task >> delete_cluster
