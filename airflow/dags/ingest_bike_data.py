#!/usr/bin/env python3
#
# File specifying Airflow DAGs for ingesting TfL cycling data

import logging
from os import environ
from json import load
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")

# Local folder within the docker container
AIRFLOW_HOME = environ.get("AIRFLOW_HOME", "/opt/airflow/")


def get_start_date_from_time(execution_date):
    """
    Gets the start date of the extracted data range. 

    :param execution_date: Date of scheduled airflow run. A datetime object is 
        automatically passed to this argument by airflow

    :return: Date in "YYYYMM" format.
    """

    # Some weeks have +1 or -1 days, so adjust for this
    if execution_date.date() == date(2017, 8, 22):
        days = 7

    elif execution_date.date() == date(2017, 8,  1):
        days = 5

    else:
        days = 6

    return (execution_date - timedelta(days=days)).strftime("%Y%m")


def get_usage_file_names_from_time(execution_date):
    """
    Each (weekly) usage data file in the source bucket has a complex naming pattern including:
        - An ID incrementing with each week
        - The start date of the file, e.g. 04Jan2017
        - The end date of the file, e.g. 10Jan2017

    Here, we calculate these fields from the execution date.

    Some datasets have inconsistent names, and some are not even saved as CSV.

    :param execution_date: Date of scheduled airflow run. A datetime object is 
        automatically passed to this argument by airflow
    
    :return: The formatted file name for the airflow run"s time period as a string
        and the dataset start date in "YYYYMM" format.
    """

    # 2017 IDs start at 38 - this is added to the number of weeks since the first data chunk
    dataset_id = 38 + ((execution_date.date() - date(2017, 1, 3)).days // 7)

    # Two weeks in a row have the same ID, so reduce by one after this point
    if execution_date.date() >= date(2021, 1, 5):
        dataset_id -= 1
    
    logging.info(f"The dataset's ID is {dataset_id}")

    # Naming convention has spaces for certain dates
    after_id, after_journey, after_data, after_extract = "", "", "", ""
    space_char = "%20"

    if dataset_id in (50, 51, 52):
        after_id, after_journey, after_data, after_extract = space_char, space_char, space_char, space_char
    elif dataset_id == 55:
        after_data = space_char
    elif dataset_id == 56:
        after_extract = space_char

    journey_data_extract = f"{after_id}Journey{after_journey}Data{after_data}Extract{after_extract}"

    # Get the start and end ranges of the data (execution_date refers to the end of the data range)

    dataset_start_range = (execution_date - timedelta(days=6)).strftime("%d%b%Y")
    dataset_end_range = execution_date.strftime("%d%b%Y")

    # Some weeks have 6 or 8 days according to the data source, make adjustments
    ranges_with_early_starts = [date(2017, 8,  day) for day in [8, 15, 22]]
    ranges_with_early_ends = [date(2017, 8,  day) for day in [1, 8, 15]]
    
    if execution_date.date() in ranges_with_early_starts:
        dataset_start_range = (execution_date - timedelta(days=7)).strftime("%d%b%Y")  

    if execution_date.date() in ranges_with_early_ends:
        dataset_end_range = (execution_date - timedelta(days=1)).strftime("%d%b%Y")

    # In 2018, June is used instead of Jun and July in place of Jul
    if 100 < dataset_id < 150:
        dataset_start_range = dataset_start_range.replace("Jun", "June").replace("Jul", "July")
        dataset_end_range = dataset_end_range.replace("Jun", "June").replace("Jul", "July")

    logging.info(f"Will extract data for the time period {dataset_start_range} to {dataset_end_range}")

    # Dataset 49 is saved as a .xlsx file
    if dataset_id == 49:
        file_ext = ".xlsx"
    else:
        file_ext = ".csv"

    # Get the properly formatted dataset name
    formatted_dataset_name = f"{dataset_id}{journey_data_extract}{dataset_start_range}-{dataset_end_range}{file_ext}"
    logging.info(f"Formatted dataset name: {formatted_dataset_name}")

    return formatted_dataset_name


def format_to_csv(file_dir, file_name):
    """
    Some datasets are saved as XLSX rather than CSV. This function converts them
    to CSV in the case they are not already.

    :param file_dir: The directory in which the file is stored.

    :param file_name: The name of the file to be converted.

    :return: The name of the CSV file, saved within `file_dir`.
    """

    # Convert from excel if necessary
    if file_name.lower().endswith(".xlsx"): 

        csv_file_name = file_name.replace(".xlsx", ".csv")

        # Import here as we don't want to import at top of file for Airflow
        import pandas as pd
        
        df = pd.read_excel(f"{file_dir}/{file_name}")
        df.to_csv(f"{file_dir}/{csv_file_name}", sep=",", index=False)

    elif file_name.lower().endswith(".csv"):

        csv_file_name = file_name

    else:
        raise TypeError("File ends with an unknown extension.")

    logging.info(f"CSV file name: {csv_file_name}")

    # Remove spaces in file name if there are any
    csv_file_name_without_spaces = csv_file_name.replace("%20", "").replace(" ", "")
    logging.info(f"CSV file name without spaces: {csv_file_name_without_spaces}")

    if csv_file_name != csv_file_name_without_spaces:

        # Import here as we don't want to import at top of file for Airflow
        from shutil import copyfile

        copyfile(csv_file_name, csv_file_name_without_spaces)

    return csv_file_name_without_spaces


def format_to_parquet(csv_file_dir, csv_file_name, column_types=None):
    """
    Converts a CSV file to a parquet file. Parquet files are ideal because they allow for
    efficient data compression while allowing queries to read only the necessary columns.

    :param csv_file_dir: The directory in which the CSV file is stored.

    :param csv_file_name: The name of the CSV file to be converted.

    :param column_types: If given, specifies the schema of the parquet file
        to be written.

    :return: The name of the parquet file, saved within `csv_file_dir`.
    """

    if not csv_file_name.lower().endswith(".csv"):
        raise TypeError("Can only convert CSV files to parquet")

    # Import here as we don't want to import at top of file for Airflow
    from pyarrow.csv import read_csv, ConvertOptions
    from pyarrow.parquet import write_table

    # Convert CSV to parquet
    parquet_file_name = csv_file_name.replace(".csv", ".parquet")
    logging.info(f"Parquetised dataset name: {parquet_file_name}")

    # If given schema, give to pyarrow reader instead of inferring column types
    if column_types is not None:
        convert_options = ConvertOptions(column_types=column_types)
    else:
        convert_options = None

    arrow_table = read_csv(f"{csv_file_dir}/{csv_file_name}", convert_options=convert_options)
    
    write_table(arrow_table, f"{csv_file_dir}/{parquet_file_name}")

    return parquet_file_name


def reformat_locations_xml(xml_file_dir, xml_file_name):
    """
    Converts a live XML corresponding to bike locations and extracts relevant data fields.

    :param xml_file_dir: The directory in which the XML file to be processed is stored.

    :param xml_file_name: The name of the XML file to be processed.  

    :return: The name of the CSV file saved within `xml_file_dir`. 
    """

    if not xml_file_name.lower().endswith(".xml"):
        raise TypeError("Can only extract data from XML files")

    # Modules for converting xml to csv
    # Import here as we don't want to import at top of file for Airflow
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
    schedule_interval = "0 23 * * 2",  # Every week
    catchup = True,
    max_active_runs = 3,
    tags = ["tfl_digest", "bike_usage"],
    start_date = datetime(2017, 1, 3, 20),
    end_date = datetime(2022, 1, 4, 20),  
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0
    }
) as ingest_bike_usage:
    """
    This DAG ingests the primary dataset from TfL. Currently, it is specified to
    run from the start of 2017 until the start of 2022. The DAG runs weekly to get
    the most recent data, and moves them to the GCS. The data is released on Tuesdays.

    See `docs/data_sources.md` for more information on the external dataset.
    """

    # Need to get the start date to save the output to the correct GCS folder
    get_start_date = PythonOperator(
        task_id = "get_start_date",
        python_callable = get_start_date_from_time
    )

    data_date = "{{ ti.xcom_pull(task_ids='get_start_date') }}"
    logging.info(f"Start date of the file's range (YYYYMM): {data_date}")

    # Get the name of the file using the date of the DAG run
    get_file_names = PythonOperator(
        task_id = "get_file_names",
        python_callable = get_usage_file_names_from_time
    )

    # Get the return value of the get_file_names task
    file_name = "{{ ti.xcom_pull(task_ids='get_file_names') }}"
    logging.info(f"Pulled csv name: {file_name}")

    # The week"s data is downloaded to the local machine
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf 'https://cycling.data.tfl.gov.uk/usage-stats/{file_name}' > {AIRFLOW_HOME}/{file_name}"
    )

    # A minority of files are saved as .xlsx rather than .csv
    # This function will convert them to CSV format
    convert_to_csv = PythonOperator(
        task_id = "convert_to_csv",
        python_callable = format_to_csv,
        op_kwargs = {
            "file_dir": AIRFLOW_HOME,
            "file_name": file_name
        }
    )

    # Get the new dataset name after conversion to parquet
    csv_file_name = "{{ ti.xcom_pull(task_ids='convert_to_csv') }}"
    logging.info(f"Pulled CSV name: {csv_file_name}")

    # Remove spaces from file header
    new_header = "Rental_Id,Duration,Bike_Id,End_Date,EndStation_Id,EndStation_Name,Start_Date,StartStation_Id,StartStation_Name"

    convert_csv_header = BashOperator(
        task_id = "convert_csv_header",
        bash_command = f"""new_header='{new_header}'
                           sed -i "1s/.*/$new_header/" {AIRFLOW_HOME}/{csv_file_name}
                           head {AIRFLOW_HOME}/{csv_file_name}
                           tail {AIRFLOW_HOME}/{csv_file_name}
                        """
    )

    # We convert to the columnar parquet format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_file_dir": AIRFLOW_HOME,
            "csv_file_name": csv_file_name,
            "column_types": load(open(f"{AIRFLOW_HOME}/schema/journey_schema.json", "r"))
        }
    )

    # Get the new dataset name after conversion to parquet
    parquet_file_name = "{{ ti.xcom_pull(task_ids='convert_to_parquet') }}"
    logging.info(f"Pulled parquet name: {parquet_file_name}")

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{parquet_file_name}",
        dst = f"rides_data/{data_date}/{parquet_file_name}",
        bucket = GCP_GCS_BUCKET
    )

    # Setup dependencies
    get_file_names >> download_file_from_https >> convert_to_csv >> convert_csv_header
    convert_csv_header >> convert_to_parquet >> transfer_data_to_gcs
    get_start_date >> transfer_data_to_gcs


with DAG(
    dag_id = "ingest_bike_locations",
    schedule_interval = "0 0 1 * *",
    catchup = False,
    max_active_runs = 1,
    tags = ["tfl_digest", "bike_locations"],
    start_date = datetime(2017, 2, 1),
    default_args = {
        "owner": "airflow",
        "depends_on_past": True,
        "retries": 0
    }
) as ingest_bike_locations:
    """
    This DAG extracts static location information from a live data source. We 
    extract data monthly to make sure we have the up-to-date cycle station 
    information, but we don't extract the live fields.

    See `docs/data_sources.md` for more information on the external dataset.
    """

    # The name of the live dataset file to ingest
    # We could give this as a parameter to the DAG instead
    xml_file_name = "livecyclehireupdates.xml"
    xml_file_url = "https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/"

    # The live dataset is downloaded as an XML
    # We only extract the static data concerning the bike pickup/dropoff locations
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf '{xml_file_url}{xml_file_name}' > {AIRFLOW_HOME}/{xml_file_name}"
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
    
    # Setup dependencies
    download_file_from_https >> convert_from_xml >> convert_to_parquet  >> transfer_data_to_gcs
