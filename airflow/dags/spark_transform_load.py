#!/usr/bin/env python3
#
# File specifying Airflow DAGs for running spark jobs on a GCP Dataproc cluster

import logging
from os import environ
from datetime import datetime
from json import load

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, \
                                                              BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, \
                                                              DataprocSubmitPySparkJobOperator, \
                                                              DataprocDeleteClusterOperator

from ingest_weather_data import get_previous_month_as_yyyymm


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")
GCP_PROJECT_DATAPROC_CLUSTER_NAME = environ.get("GCP_PROJECT_DATAPROC_CLUSTER_NAME", "bikes-cluster")
GCS_REGION = environ.get("GCS_REGION", "europe-north1")

# Local folder within the docker container
AIRFLOW_HOME = environ.get("AIRFLOW_HOME", "/opt/airflow/")
SPARK_HOME = environ.get("SPARK_HOME", "/opt/spark/")

# Whether to setup/deconstruct cluster resources or assume they are already present
CREATE_INFRASTRUCTURE = environ.get("CREATE_INFRASTRUCTURE", "True").lower() in ("true", "1", "t")


def get_cluster_setup_task():
    """
    Creates a `DataprocCreateClusterOperator` task for creating a lightweight Dataproc 
    cluster based on GCP variables.

    :return: A `DataprocCreateClusterOperator` task.
    """

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
        job_arguments += additional_arguments

    return DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_spark_job_task",
        main = f"gs://{GCP_GCS_BUCKET}/spark/{file_name}",
        arguments = job_arguments,
        cluster_name = GCP_PROJECT_DATAPROC_CLUSTER_NAME,
        region = GCS_REGION,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"]
    )


def get_cluster_teardown_task():
    """
    Destroys a cluster using a `DataprocDeleteClusterOperator` task. The trigger
    rule is set to "all_done", meaning it will be carried out even if the PySpark
    job fails.

    :return: A `DataprocDeleteClusterOperator` task.
    """

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


def create_bigquery_table(table_name, schema_fields, time_partitioning=None):
    """
    Create a BigQuery table within an existing dataset.

    :param table_name: The name of the table to be created.

    :param schema_fields: A list of fields, see `BigQueryCreateEmptyTableOperator`
        for more details. A basic `schema_fields` object would be a list of 
        dictionaries that each have the "name", "type" and "mode" keys. The name
        indicates the column name, the type is the variable type (e.g. "INTEGER",
        "STRING"), and the mode indicates the column mode (e.g. "REQUIRED", "NULLABLE").

    :param time_partitioning: Table resources, see `BigQueryCreateEmptyTableOperator`.
        Can be used to indicate table partitioning, for instance by suppling
        the following object to the `time_partitioning` argument:
        `time_partitioning = {"type": "MONTH", "field": "end_timestamp"}`

    :return A `BigQueryCreateEmptyTableOperator` task.
    """

    return BigQueryCreateEmptyTableOperator(
        task_id = f"create_{table_name}",
        project_id = GCP_PROJECT_ID,
        dataset_id = BIGQUERY_DATASET,
        table_id = table_name,
        schema_fields = schema_fields,
        time_partitioning = time_partitioning
    )


with DAG(
    dag_id = "setup_bigquery",
    schedule_interval = "@once",  # One-time setup
    catchup = False,
    max_active_runs = 1,
    tags = ["spark_setup", "create_warehouse"],
    start_date = days_ago(1),
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    }
) as setup_bigquery:
    """
    Creates a DAG for setting up BigQuery. Creates a BigQuery dataset before
    creating tables for each of the datasets. Empty tables are created for the
    datasets that will be ingested by the "transform_dag". Complete tables are
    created for the location data and timestamp table.

    The DAG runs a single time, after which data just needs to be appended to the 
    tables as it is released, using the "transform_dag" DAG. 

    If the `CREATE_INFRASTRUCTURE` variable is True, tasks are created before
    and after the Dataproc PySpark job submission to setup and teardown a
    Dataproc cluster. 
    """

    # Create the empty BigQuery dataset
    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = "create_bigquery_dataset",
        project_id = GCP_PROJECT_ID,
        dataset_id = BIGQUERY_DATASET,
        location = GCS_REGION
    )

    # Now create the empty tables within the dataset, using saved json schema
    create_fact_journey = create_bigquery_table(
        table_name = "fact_journey", 
        schema_fields = load(open(f"{AIRFLOW_HOME}/schema/fact_schema.json", "r")),
        time_partitioning = {"type": "MONTH", "field": "end_timestamp"}
    )

    create_dim_rental = create_bigquery_table(
        table_name = "dim_rental", 
        schema_fields = load(open(f"{AIRFLOW_HOME}/schema/rental_schema.json", "r"))
    )

    create_dim_weather = create_bigquery_table(
        table_name = "dim_weather", 
        schema_fields = load(open(f"{AIRFLOW_HOME}/schema/weather_schema.json", "r")),
        time_partitioning = {"type": "MONTH", "field": "timestamp"}
    )

    # Create tables after creating the dataset
    create_bigquery_dataset >> create_fact_journey
    create_bigquery_dataset >> create_dim_rental
    create_bigquery_dataset >> create_dim_weather

    # Tables are ready now, so we can upload the pyspark file and submit it to dataproc
    upload_pyspark_file = get_pyspark_upload_task("transform_load.py")

    # All the dependencies are ready, submit the spark job
    submit_dataproc_spark_job_task = get_cluster_job_submission_task(
        "transform_load.py", 
        additional_arguments = ["", "setup_database"]
    )

    # Set dependencies for submit_dataproc_spark_job_task
    upload_pyspark_file >> submit_dataproc_spark_job_task
    create_fact_journey >> submit_dataproc_spark_job_task
    create_dim_rental >> submit_dataproc_spark_job_task
    create_dim_weather >> submit_dataproc_spark_job_task

    # If the dataproc cluster has not already been created, we can set
    # it up and tear it down before and after submitting the job, respectively
    if CREATE_INFRASTRUCTURE:
        create_cluster = get_cluster_setup_task()
        delete_cluster = get_cluster_teardown_task()
        create_cluster >> submit_dataproc_spark_job_task >> delete_cluster


def create_transform_dag(dag_id, day_of_month=10):
    """
    Creates a DAG for processing either weather or journey data.

    If the `CREATE_INFRASTRUCTURE` variable is True, tasks are created before
    and after the Dataproc PySpark job submission to setup and teardown a
    Dataproc cluster. 

    :param dag_id: This argument is passed as an argument to the PySpark script,
        it dictates the behaviour of the workflow. A value of "transform_load_weather"
        means that the workflow will transform the weather data and load it to BigQuery,
        a value of "transform_load_journeys" does the same for the journeys data.

    :param day_of_month: The day of the month on which to perform the DAG. The DAG
        will run every month on this day. 

    :return: Returns a DAG for transforming either weather or journey data.
    """

    transform_dag = DAG(
        dag_id = dag_id,
        schedule_interval = f"0 0 {day_of_month} * *",  # Transform/load previous month's data on the 10th
        catchup = True,
        max_active_runs = 1,
        tags = ["spark_transform", "update_warehouse", "journey_data"],
        start_date = datetime(2017, 1, day_of_month),  # datetime(2017, 1, day_of_month)
        end_date = datetime(2022, 1, day_of_month),
        default_args = {
            "owner": "airflow",
            "depends_on_past": True,
            "retries": 0
        }
    )
    
    with transform_dag:

        upload_pyspark_file = get_pyspark_upload_task("transform_load.py")

        # Get the month (in YYYYMM format) of the current DAG run
        get_previous_month = PythonOperator(
            task_id = "get_previous_month",
            python_callable = get_previous_month_as_yyyymm
        )

        data_date = "{{ ti.xcom_pull(task_ids='get_previous_month') }}"
        logging.info(f"YYYYMM: {data_date}")

        # Submit dataproc job with an additional argument: 
        # The year and month ("YYYYMM") of the data to be transformed and loaded
        # If this is the first time running this DAG, use overwrite mode for BigQuery
        submit_dataproc_spark_job_task = get_cluster_job_submission_task(
            "transform_load.py", 
            additional_arguments = [data_date, dag_id]
        )

        # These are the dependencies of the dataproc job
        upload_pyspark_file >> submit_dataproc_spark_job_task
        get_previous_month >> submit_dataproc_spark_job_task

        # If the dataproc cluster has not already been created, we can set
        # it up and tear it down before and after submitting the job, respectively
        if CREATE_INFRASTRUCTURE:
            create_cluster = get_cluster_setup_task()
            delete_cluster = get_cluster_teardown_task()
            create_cluster >> submit_dataproc_spark_job_task >> delete_cluster

    return transform_dag


# The journeys data depends on the weather data being transformed, so we perform the
# journeys data transformation the day after the weather data. The transform/load 
# steps are performed after ~10 days into the month to ensure that the final week of
# the previous month's journeys data has been digested to GCS.
transform_load_weather = create_transform_dag("transform_load_weather", day_of_month=9)
transform_load_journeys = create_transform_dag("transform_load_journeys", day_of_month=10)
