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


def get_usage_file_names_from_time(execution_date):
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
    logging.info(f"The dataset's ID is {dataset_id}")

    # Get the start and end ranges of the data (execution_date refers to the end of the data range)
    dataset_start_range = (execution_date - timedelta(days=6)).strftime("%d%b%Y")
    dataset_end_range = execution_date.strftime("%d%b%Y")
    logging.info(f"Will extract data for the time period {dataset_start_range} to {dataset_end_range}")

    # Get the properly formatted dataset name
    formatted_dataset_name = f"{dataset_id}JourneyDataExtract{dataset_start_range}-{dataset_end_range}.csv"
    logging.info(f"Formatted dataset name: {formatted_dataset_name}")

    return formatted_dataset_name

def format_to_parquet(csv_file_dir, csv_file_name):
    """
    Converts a CSV file to a parquet file. Parquet files are ideal because they allow for
    efficient data compression while allowing queries to read only the necessary columns.

    :param csv_file_name: The CSV file to be converted.
    """

    if not csv_file_name.lower().endswith(".csv"):
        raise TypeError("Can only convert CSV files to parquet")

    # Import pyarrow within task
    from pyarrow.csv import read_csv
    from pyarrow.parquet import write_table

    # Convert CSV to parquet
    parquet_file_name = csv_file_name.replace(".csv", ".parquet")
    logging.info(f"Parquetised dataset name: {parquet_file_name}")

    write_table(read_csv(f"{csv_file_dir}/{csv_file_name}"), f"{csv_file_dir}/{parquet_file_name}")

    return parquet_file_name


def reformat_locations_xml(xml_file_dir, xml_file_name):
    """
    Converts a live XML corresponding to bike locations and extracts relevant data fields.

    :param xml_file_name: The name of the XML file to be processed.  
    """

    if not xml_file_name.lower().endswith(".xml"):
        raise TypeError("Can only extract data from XML files")

    # Modules for converting xml to csv
    import csv
    from xml.etree import ElementTree

    # Get the root of the XML
    locations_tree = ElementTree.parse(f"{xml_file_dir}/{xml_file_name}")
    stations = locations_tree.getroot()

    # These are the variables we wish to extract
    station_vars = ["id", "name", "terminalName", "lat", "long"]

    # Create replacement CSV
    csv_file_name = xml_file_name.replace(".xml", ".csv")
    logging.info(f"CSV dataset name: {csv_file_name}")

    with open(f"{xml_file_dir}/{csv_file_name}", "w") as output_file:

        # Write header to CSV
        output_csv = csv.writer(output_file)
        output_csv.writerow(station_vars)
        
        # Loop through each node (station) and find each variable, write to CSV
        logging.info("Preview of XML to CSV conversion:")
        logging.info(station_vars)

        for i, station in enumerate(stations):

            csv_row = [station.find(station_var).text for station_var in station_vars]

            if i < 5:
                logging.info(csv_row)

            output_csv.writerow(csv_row)

    return csv_file_name


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
    get_file_names = PythonOperator(
        task_id = "get_file_names",
        python_callable = get_usage_file_names_from_time
    )

    # Get the return value of the get_file_names task
    csv_file_name = "{{ ti.xcom_pull(task_ids='get_file_names') }}"
    logging.info(f"Pulled csv name: {csv_file_name}")

    # The week"s data is downloaded to the local machine
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf https://cycling.data.tfl.gov.uk/usage-stats/{csv_file_name} > {AIRFLOW_HOME}/{csv_file_name}"
    )

    # Remove spaces from file header
    convert_csv_header = BashOperator(
        task_id = "convert_csv_header",
        bash_command = f"""new_header='Rental_Id,Duration,Bike_Id,End_Date,EndStation_Id,EndStation_Name,Start_Date,StartStation_Id,StartStation_Name'
                           sed -i "1s/.*/$new_header/" {AIRFLOW_HOME}/{csv_file_name}
                        """
    )

    # We convert to the columnar parquet format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_file_dir": AIRFLOW_HOME,
            "csv_file_name": csv_file_name
        }
    )

    # Get the new dataset name after conversion to parquet
    parquet_file_name = "{{ ti.xcom_pull(task_ids='convert_to_parquet') }}"
    logging.info(f"Pulled parquet name: {parquet_file_name}")

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{parquet_file_name}",
        dst = f"rides_data/{parquet_file_name}",
        bucket = GCP_GCS_BUCKET
    )

    get_file_names >> download_file_from_https >> convert_csv_header >> convert_to_parquet >> transfer_data_to_gcs


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
        "retries": 0
    }
) as ingest_bike_locations:

    xml_file_name = "livecyclehireupdates.xml"
    
    # The live dataset is downloaded as an XML
    # We only extract the static data concerning the bike pickup/dropoff locations
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/{xml_file_name} > {AIRFLOW_HOME}/{xml_file_name}"
    )

    # Data is an XML, so we extract the desired fields into a CSV
    convert_from_xml = PythonOperator(
        task_id = "convert_from_xml",
        python_callable = reformat_locations_xml,
        op_kwargs = {
            "xml_file_dir": AIRFLOW_HOME,
            "xml_file_name": xml_file_name
        }
    )

    csv_file_name = xml_file_name.replace(".xml", ".csv")

    # We convert to the columnar parquet format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_file_dir": AIRFLOW_HOME,
            "csv_file_name": csv_file_name
        }
    )

    parquet_file_name = csv_file_name.replace(".csv", ".parquet")

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{parquet_file_name}",
        dst = f"locations_data/{parquet_file_name}",
        bucket = GCP_GCS_BUCKET
    )

    download_file_from_https >> convert_from_xml >> convert_to_parquet  >> transfer_data_to_gcs
