Data ingestion
==============

# Airflow

Airflow should now be running within a container on your local system or VM. You can use the CLI interface or go to `localhost:8080` to use the graphical interface. Regardless, the DAGs folder (`airflow/dags`) is mounted within the container at `/opt/airflow/dags`. There are three DAG files, corresponding to eight DAGs:
- __ingest_bike_locations__: From `ingest_bike_data.py` - Ingests data on the cycle stations.
- __ingest_bike_usage__: From `ingest_bike_data.py` - Ingests data on the bike journeys.
- __ingest_rainfall_weather__: From `ingest_weather_data.py` - Ingests rainfall weather data.
- __ingest_tasmin_weather__: From `ingest_weather_data.py` - Ingests minimum temperature weather data.
- __ingest_tasmax_weather__: From `ingest_weather_data.py` - Ingests maximum temperature weather data.
- __setup_bigquery__: From `spark_transform_load.py` - Creates static BigQuery tables and tables to be populated.
- __transform_load_journeys__: From `spark_transform_load.py` - Uses spark to transform the journey data and load it to BigQuery.
- __transform_load_weather__: From `spark_transform_load.py` - Uses spark to transform the weather data and load it to BigQuery.

In this document, the DAGs with the `ingest_` prefix will be discussed. The remaining DAGs will be explained in `docs/3_data_transformation.md`. Each of the ingestion scripts pull a dataset from some location (HTTP or FTP) and uploads it to Google Cloud Storage (GCS). There is some transformation of the data involved (e.g. converting to parquet), so this is a small ETL step in its own right. 

## Ingesting locations data

The locations data can be found as an XML available from a HTTPS TfL feed. The data is obtained at the start of each month, in case there have been any updates to the bike station locations. The first step in the DAG, displayed below, is to download these data within the Airflow container. The data is then converted from XML to CSV and from CSV to parquet, before being uploaded to the GCS bucket. It would have been slightly more efficient to convert directly from XML to parquet, so the DAG could be improved in the future.

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/dags/ingest_bike_locations.png?raw=true" />
</p>

## Ingesting usage data

The usage data can be found in a HTTPS bucket available from TfL. A new file is saved every Tuesday, so the DAG is run each week to obtain the new dataset. In general, the data is saved as a CSV in a common naming convention. Unfortunately, there are some inconsistencies with the historical data. Firstly, the naming convention changes over time, for instance some names have additional spaces. Additionally, at some points the data interval changes (e.g. some files contain +-1 day of data). Finally, one of the files is saved as an XLSX file rather than CSV, so must be converted before being processed. The functions `get_start_date_from_time` and `get_usage_file_names_from_time` are designed to deal with these inconsistencies in order to get the correct date range and file name for each week's data deposition.

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/dags/ingest_bike_usage.png?raw=true" />
</p>

While most files are saved as CSV, the next step (`convert_to_csv`) checks if the file is XLSX, and converts it to CSV if so. The `convert_csv_header` step removes spaced from the header line of the CSV file in preparation for parquetisation, after which the weekly data can be transferred to the GCS. Each week's data is saved in a monthly folder (named in the format YYYYMM, obtained from the `get_start_date` DAG step), which simplifies retrieval of the datasets by Spark simple in a later step.

## Ingesting weather data

The weather data is a lot more consistent than the journey data, but this ingestion step has its own challenges. The function `create_weather_dag` is used to create three DAGs, one for each set of weather data to be obtained (rainfall, tasmin, tasmax). The DAG for each data type is identical, differing only by the dataset to be ingested, so we only show one below. 

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/dags/ingest_rainfall_weather.png?raw=true" />
</p>

Near the start of each month, the DAG obtains the weather data for the previous month. The date of the previous month and the name of the file is calcualted before the file can be downloaded from FTP. As mentioned in `docs/1_setup.md`, the `get_ftp_dataset` function requires a CEDA username and FTP password in order to download the data. An account must be setup prior to running this DAG.

Next, we use the function `reformat_netcdf` to convert the data from netCDF format to CSV. The netCDF file contains numpy dataframes corresponding to a 1km by 1km grid of the UK, at each point there is a data observation for each day of the month. However, we are only interested in data for London and, more specifically, we are only interested in the points nearest to the bike stations. We could have uploaded the entire set of UK weather data to the GCS and dealt with this in the Spark transformation step, but this would mean storing a large amount of unuseful data. Instead, the pipeline filters the data such that only the required observations remain.

We get the locations data, obtained by the `ingest_bike_locations` DAG, from the GCS. We then iterate through each cycle station (only ~800 locations) and, in each case, select the weather data corresponding to the closest point (by euclidean distance) on the 1km by 1km grid. These data are combined to a pandas DataFrame before being saved as CSV.

The CSV is converted to parquet and uploaded to the GCS in a folder named for the previous month, like the journey data.

While most of the transformation is done by Spark, there were some minor modifications made to the TfL data (e.g. conversion to parquet) and major filtering applied to the weather data at this ingestion stage. Ideally, this processing should be run separately to Airflow, as discussed in `docs/limitations_and_directions.md`.  

## Running the DAGs

You can now activate the DAGs, starting with the locations data before turning on the journey and weather data DAGs. These digest the data for each week/month and upload the data to the GCS. They are set to "catchup", i.e. they get historical data since the `start_date`. 
