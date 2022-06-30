import logging
from os import environ
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")

# Local folder within the docker container
AIRFLOW_HOME = environ.get("AIRFLOW_HOME", "/opt/airflow/")


def get_usage_filenames_from_time(execution_date):
    """
    Each (weekly) usage data file in the source bucket has a complex naming pattern including:
        - An ID incrementing with each week
        - The start date of the file, e.g. 04Jan2017
        - The end date of the file, e.g. 10Jan2017

    Here, we calculate these fields from the execution date

    :param execution_date: Date of scheduled airflow run. A datetime object is automatically passed to 
        this argument by airflow
    
    :return: The formatted file name for the airflow run"s time period as a string.
    """

    # 2017 IDs start at 38 - this is added to the number of weeks since the first data chunk
    dataset_id = 38 + ((execution_date - datetime(2017, 1, 3, 20, tzinfo=execution_date.tzinfo)).days // 7)

    # Get the start and end ranges of the data (execution_date refers to the end of the data range)
    dataset_start_range = (execution_date - timedelta(days=6)).strftime("%d%b%Y")
    dataset_end_range = execution_date.strftime("%d%b%Y")

    # Get the properly formatted dataset name
    return f"{dataset_id}JourneyDataExtract{dataset_start_range}-{dataset_end_range}.csv"

def format_to_parquet(csv_filename):
    """
    Converts a CSV file to a parquet file. Parquet files are ideal because they allow for
    efficient data compression while allowing queries to read only the necessary columns.

    :param csv_filename: The CSV file to be converted.
    """

    if not csv_filename.lower().endswith(".csv"):
        raise TypeError("Can only convert CSV files to parquet")

    # Import pyarrow within task
    from pyarrow.csv import read_csv
    from pyarrow.parquet import write_table

    # Convert CSV to parquet
    write_table(read_csv(csv_filename), csv_filename.replace(".csv", ".parquet"))

def reformat_locations_xml(xml_filename):
    """
    Converts a live XML corresponding to bike locations and extracts relevant data fields.

    :param xml_filename: The name of the XML file to be processed.  
    """

    if not xml_filename.lower().endswith(".xml"):
        raise TypeError("Can only extract data from XML files")

    # Modules for converting xml to csv
    import csv
    from xml.etree import ElementTree

    # Get the root of the XML
    locations_tree = ElementTree.parse(xml_filename)
    stations = locations_tree.getroot()

    # These are the variables we wish to extract
    station_vars = ["id", "name", "terminalName", "lat", "long"]

    # Create replacement CSV
    with open(xml_filename.replace(".xml", ".csv"), "w") as output_file:

        # Write header to CSV
        output_csv = csv.writer(output_file)
        output_csv.writerow(station_vars)
        
        # Loop through each node (station) and find each variable, write to CSV
        for station in stations:
            output_csv.writerow([station.find(station_var).text for station_var in station_vars])


with DAG(
    dag_id = "ingest_bike_usage",
    schedule_interval = "0 20 * * 2",
    catchup = True,
    max_active_runs = 3,
    tags = ["bike_usage"],
    start_date = datetime(2017, 1, 3, 20),
    end_date = datetime(2017, 1, 24, 20),  # datetime(2022, 1, 4, 20)
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0,
    }
) as ingest_bike_usage:

    # Get the name of the file using the date of the DAG run
    get_filenames = PythonOperator(
        task_id = "get_filenames",
        python_callable = get_usage_filenames_from_time
    )

    # Get the return value of the get_filenames task
    dataset_name = "{{ ti.xcom_pull(task_ids='get_filenames') }}"
    logging.debug(f"dataset_name: {dataset_name}")

    # The week"s data is downloaded to the local machine
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf https://cycling.data.tfl.gov.uk/usage-stats/{dataset_name} > {AIRFLOW_HOME}/{dataset_name}"
    )

    # We convert to the columnar parquet format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_filename": f"{AIRFLOW_HOME}/{dataset_name}",
        },
    )

    dataset_name = dataset_name.replace(".csv", ".parquet")

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{dataset_name}",
        dst = f"rides_data/{dataset_name}",
        bucket = GCP_GCS_BUCKET
    )
    
    get_filenames >> download_file_from_https >> convert_to_parquet >> transfer_data_to_gcs


with DAG(
    dag_id = "ingest_bike_locations",
    schedule_interval = "@once",
    catchup = False,
    max_active_runs = 1,
    tags = ["bike_locations"],
    start_date = days_ago(1),
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 0,
    }
) as ingest_bike_locations:

    dataset_name = "livecyclehireupdates.xml"
    
    # The live dataset is downloaded as an XML
    # We only extract the static data concerning the bike pickup/dropoff locations
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/{dataset_name} > {AIRFLOW_HOME}/{dataset_name}"
    )

    # Data is an XML, so we extract the desired fields into a CSV
    convert_from_xml = PythonOperator(
        task_id = "convert_from_xml",
        python_callable = reformat_locations_xml,
        op_kwargs = {
            "xml_filename": f"{AIRFLOW_HOME}/{dataset_name}",
        },
    )

    dataset_name = dataset_name.replace(".xml", ".csv")
    
    # We convert to the columnar parquet format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_filename": f"{AIRFLOW_HOME}/{dataset_name}",
        },
    )

    dataset_name = dataset_name.replace(".csv", ".parquet")

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{dataset_name}",
        dst = f"locations_data/{dataset_name}",
        bucket = GCP_GCS_BUCKET
    )

    download_file_from_https >> convert_from_xml >> convert_to_parquet  >> transfer_data_to_gcs
