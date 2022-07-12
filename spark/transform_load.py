#!/usr/bin/env python3
#
# PySpark script that is used to transform/load data to BigQuery. The script runs
# in three modes, depending on the fifth argument given to the script. The data
# transformations are run depending on the fourth argument given to the script,
# which specifies the month and year. Arguments 1-3 are used to determine the 
# GCP project ID, bucket location and BigQuery dataset, respectively.

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# Get arguments specifying GCS locations
GCP_PROJECT_ID = sys.argv[1]
GCP_GCS_BUCKET = sys.argv[2]
BIGQUERY_DATASET = sys.argv[3]

# Get pipeline parameters
MONTH_YEAR = sys.argv[4]  # The month and year of the data to be processed
STAGE = sys.argv[5]  # Processing stages to be run


# Configure logger
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
curdate = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def get_column_types_from_df(df, show_num_null=True):
    """
    Prints the column types of a spark dataframe to logger.

    :param df: A spark RDD.

    :param show_num_null: If True, prints the number of `Null` values for each
        column in `df`.
    """

    # Log the datatype of each column
    for column_name in df.schema.names:
        logger.info(f"{column_name}: {df.schema[column_name].dataType}")

    # Count number of Null values for non-timestamp columns
    if show_num_null:
        logger.info("Null counts by column: ")
        df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()


def send_to_bigquery(df, additional_options=None, mode="append"):
    """
    Sends a spark RDD to BigQuery to create a new table, or append to an existing table.

    :param df: A spark RDD to be uploaded to BigQuery.

    :param additional_options: Used to add options to the BigQuery write (using spark's `.option`
        function). Must be a dictionary whose keys are the names of options for the bigquery write
        and values are the values to be set for those options. Must contain "table".

    :param mode: A string to be sent to the spark's `.mode` function. Can be set to "append" in order
        to append to a new table, or "overwrite" to create a new table or overwite an existing one.
    """

    # Log some information about the table to be written
    table = additional_options["table"]

    logger.info(f"Sending table {table} to bigquery: ")
    get_column_types_from_df(df)
    df.show()

    # Append data to pre-existing BigQuery table or create a new one
    df = df.write.format("bigquery") \
        .mode(mode) \
        .option("project", GCP_PROJECT_ID) \
        .option("dataset", BIGQUERY_DATASET) 

    # Add additional options passed to the function
    for option_name, option_value in additional_options.items():
        df = df.option(option_name, option_value)

    # Send to BigQuery
    df = df.save()


def get_timestamp_dimension(spark):
    """ 
    Creates a table mapping timestamp IDs (unix timestamp) to other time-related variables,
    including timestamp, year, month, day of month, week of year, hour and minute.

    :param spark: A SparkSession object.

    :return: A Spark RDD containing the timestamp dimension table.
    """

    # Create date dimension table
    begin_timestamp, end_timestamp = "2016-01-01 00:00", "2022-01-31 23:59"

    # Sequence is used to create a timestamp dimension with the unix_timestamp as the key
    timestamp_seq = f"sequence(to_timestamp('{begin_timestamp}'), to_timestamp('{end_timestamp}'), interval 1 minute)"

    # Create the table, including new time-related columns
    df = spark \
        .createDataFrame([(1,)], ["c"]) \
        .withColumn("timestamp", F.explode(F.expr(timestamp_seq))) \
        .withColumn("id", F.unix_timestamp("timestamp").alias("unix_timestamp").cast("int")) \
        .select("id", "timestamp") \
        .withColumn("year", F.year("timestamp").alias("year")) \
        .withColumn("month", F.month("timestamp").alias("month")) \
        .withColumn("dayofmonth", F.dayofmonth("timestamp").alias("dayofmonth")) \
        .withColumn("weekofyear", F.weekofyear("timestamp").alias("weekofyear")) \
        .withColumn("hour", F.hour("timestamp").alias("hour")) \
        .withColumn("minute", F.minute("timestamp").alias("minute"))

    # Log timestamp dimension details
    logger.debug("Timestamp dimension: ")
    df.show()
    get_column_types_from_df(df)

    return df


def get_locations_data(spark, file_name):
    """ 
    Extract data from the journey/usage files converted to parquet from the TfL portal. 

    :param spark: A SparkSession object.

    :param file_name: The location of a parquet file containing the journey data.

    :return: Returns the locations data as a spark dataframe.
    """

    df = spark.read.parquet(file_name).withColumnRenamed("terminalName", "terminal_name")

    # Log information about the raw table
    logger.info("Raw imported locations data:")
    df.show()
    get_column_types_from_df(df)

    # Convert to integers
    for id_col in ("id", "terminal_name"):
        df = df.withColumn(id_col, F.col(id_col).cast("int"))

    # Store latitude and longitude as custom decimal types
    df = df.withColumn("lat", F.col("lat").cast("decimal(8, 6)")) \
           .withColumn("long", F.col("long").cast("decimal(9, 6)"))

    # Log the modified RDD
    logger.info("Reformatted imported locations data:")
    df.show()
    get_column_types_from_df(df)

    return df


def get_usage_data(spark, file_name):
    """ 
    Extract data from the journey/usage files converted to parquet from the TfL portal. 

    The journey data has its variables transformed before being split into two tables. The
    key table is the "journey" fact table. An additional table "rental" is created, with 
    information on each bike rental (using the rental_id key).
    
    :param spark: A SparkSession object.

    :param file_name: The folder in which journey data is stored in parquet files.

    :return: Returns the "journey" and "rental" tables as spark dataframes.
    """

    # Read parquet
    df = spark.read.option("mergeSchema", "true").parquet(file_name)

    # Rename columns
    old_columns = df.schema.names
    new_columns = [
        "rental_id", "duration", "bike_id", "end_timestamp_string", 
        "end_station_id", "end_station_name", "start_timestamp_string", 
        "start_station_id", "start_station_name"
    ]

    for old_column, new_column in zip(old_columns, new_columns):
        df = df.withColumnRenamed(old_column, new_column)

    # Log RDD information post-renaming
    logger.info("Loaded parquet after column renaming:")
    logger.info("Renamed columns: " + str(df.schema.names))
    df.show()
    get_column_types_from_df(df)

    # Convert dates to timestamps and get the unix timestamp for matching with the timestamp dimension table
    for date_col in ["start", "end"]:
        df = df.withColumn(f"{date_col}_timestamp", 
                           F.to_timestamp(df[f"{date_col}_timestamp_string"], 
                           "dd/MM/yyyy HH:mm")
                           ) \
               .withColumn(f"{date_col}_timestamp_id", 
                           F.unix_timestamp(f"{date_col}_timestamp").alias(f"{date_col}_timestamp_id")
                           )

    # Convert the following columns to integers
    int_cols = [
        "rental_id", "duration", "bike_id", "end_station_id", 
        "start_station_id", "start_timestamp_id", "end_timestamp_id"
    ]

    for id_col in int_cols:
        df = df.withColumn(id_col, F.col(id_col).cast("int"))

    # Log the changes after type changes
    logger.info("Fact/rental table after type changes: ")
    df.show()
    get_column_types_from_df(df)

    # Create bike dimension table
    rental_dimension = df.select("rental_id", "bike_id", "duration").withColumnRenamed("rental_id", "id")
    logger.info("rental_dimension: " + str(rental_dimension.schema.names))

    # Create fact table
    fact_journey = df.select(
        "rental_id", "start_station_id", "end_station_id", "start_timestamp_id", 
        "end_timestamp_id", "start_timestamp", "end_timestamp"
    )
    
    # Log information about the final fact table
    logger.info("fact_journey: " + str(fact_journey.schema.names))
    fact_journey.show()

    return fact_journey, rental_dimension


def get_weather_data(spark, file_name, month_year):
    """
    Extract weather data stored in parquet files relating to each location extracted 
    in `get_locations_data`. 

    :param spark: A SparkSession object.

    :param file_name: The location of parquet files containing the weather data. Within 
        this directory are three subfolders, each containing parquet files relating to a 
        different type of weather data (rainfall, minimum temp, maximum temp).

    :param month_year: A string in the format "YYYYMM" that indicates the year and month
        of the dataset to be processed. Used to find the data in GCS.

    :return: Returns the weather data as a spark dataframe and a modified version of 
        the input table `fact_journey` which contains a new key relating it to the 
        weather data.
    """

    # There is a folder for each weather variable, load as temporary view
    # They are split by folders with the format YYYYMM, so get the folder for the correct (previous) month
    for weather_type in ("rainfall", "tasmin", "tasmax"):
        spark.read.parquet(f"{file_name}/{weather_type}/{month_year}") \
            .createOrReplaceTempView(f"{weather_type}_from_parquet")

    # Combine the views together into a weather dimension table
    weather_dimension = spark.sql("""
        SELECT rainfall.location_id, rainfall.time, rainfall.rainfall, tasmin.tasmin, tasmax.tasmax
        FROM rainfall_from_parquet AS rainfall
        LEFT JOIN tasmin_from_parquet AS tasmin
            ON rainfall.location_id = tasmin.location_id AND rainfall.time = tasmin.time
        LEFT JOIN tasmax_from_parquet AS tasmax
            ON rainfall.location_id = tasmax.location_id AND rainfall.time = tasmax.time
    """)

    # Get the timestamp and location IDs that relate to the time dimension and location dimension, respectively
    weather_dimension = weather_dimension \
        .withColumn("timestamp_id", F.unix_timestamp("time").alias("timestamp_id")) \
        .withColumn("location_id", F.col("location_id").cast("int")) \
        .withColumn("timestamp_id", F.col("timestamp_id").cast("int")) \
        .withColumnRenamed("time", "timestamp")
                                         
    # Create a new id column that concatenates the timestamp and location IDs by an underscore
    weather_dimension = weather_dimension \
        .withColumn("id", F.concat_ws("_", F.col("location_id"), F.col("timestamp_id"))) \
        .select("id", "location_id", "timestamp_id", "timestamp", "rainfall", "tasmin", "tasmax") 

    # Log information about the final weather table
    logger.info("Modified weather dataframe: ")
    print(weather_dimension.show())
    get_column_types_from_df(weather_dimension)

    return weather_dimension


def get_weather_ids(spark, fact_journey, weather_dimension, timestamp_dimension):
    """
    Creates an ID in the fact table relating to the weather dimension table.

    :param spark: A SparkSession object.

    :param fact_journey: A table containing information on each journey. This is the 
        fact table, so we will create a new key within this table relating it to the 
        weather table.

    :param weather_dimension: A table containing weather variables for each day for
        each cycle station. Will be joined to the `fact_journey` table in order to
        add the ID of the `weather_dimension` table.

    :param timestamp_dimension: A table containing a mapping of timestamp IDs 
        (unix timestamps) to time-related variables such as year, month and day. 
        Used to join the `fact_journey` and `dim_weather` tables.
    
    :return: Returns the `fact_journey` table with additional columns for the "start"
        and "end" weather IDs. 
    """

    # Get a version of the timestamp dimension table with the required columns for making the joins
    timestamp_dimension \
        .select("id", "year", "month", "dayofmonth") \
        .withColumnRenamed("id", "timestamp_id") \
        .createOrReplaceTempView("timestamp_dimension_to_join")

    # Join weather data to the time table to prepare for joining with the fact table
    weather_dimension \
        .withColumnRenamed("id", "weather_id") \
        .createOrReplaceTempView("weather_dimension_to_join")

    spark.sql("""
        SELECT *
        FROM weather_dimension_to_join AS wdj
        LEFT JOIN timestamp_dimension_to_join AS tdj
            ON wdj.timestamp_id = tdj.timestamp_id
    """).createOrReplaceTempView("weather_dimension_time")

    logger.info("Weather data joined to time: ")
    spark.sql("SELECT * FROM weather_dimension_time LIMIT 10").show()

    # Below we create an ID mapping the fact table to the weather dimension
    # There are two locations/times for each journey (start and end)
    # So, we create a key for both the start and the end mapping each to the correct weather

    fact_cols_to_keep = [
        "rental_id", "start_station_id", "end_station_id", "start_timestamp_id", 
        "end_timestamp_id", "start_timestamp", "end_timestamp"
    ]
    
    # Setup separate fact tables for the start and end IDs, to be rejoined later
    fact_journey.createOrReplaceTempView("start_fact_journey")
    fact_journey.createOrReplaceTempView("end_fact_journey")

    # Join the weather data twice, once for each end of the journey
    for journey_side in ("start", "end"):

        logger.info(f"Getting weather ID for {journey_side} location")

        # Join the tables
        spark.sql(f"""
            SELECT *
            FROM {journey_side}_fact_journey AS fj
            INNER JOIN timestamp_dimension_to_join AS tdj
                ON fj.{journey_side}_timestamp_id = tdj.timestamp_id
        """).createOrReplaceTempView(f"{journey_side}_fact_journey_time_joined")

        # Finally, join weather to the fact table to get the ID
        fact_journey_weather_joined = spark.sql(f"""
            SELECT * 
            FROM {journey_side}_fact_journey_time_joined AS fjtj
            LEFT JOIN weather_dimension_time as wdt
                ON fjtj.year = wdt.year 
                AND fjtj.month = wdt.month 
                AND fjtj.dayofmonth = wdt.dayofmonth
                AND fjtj.{journey_side}_station_id = wdt.location_id
        """)

        # Rename the weather ID to indicate the side of the journey that was joined
        fact_journey_weather_joined = fact_journey_weather_joined \
            .withColumnRenamed("weather_id", f"{journey_side}_weather_id")

        # Log the joined tables
        logger.info(f"{journey_side} journey data joined to the weather dimension: ")
        fact_journey_weather_joined.show()
 
        # Keep the new column relating to the weather dimension
        fact_journey_weather_joined = fact_journey_weather_joined \
            .select(*(fact_cols_to_keep + [f"{journey_side}_weather_id"]))
        
        # Log the selected table and create an SQL view
        logger.info(f"Fact table joined to weather by {journey_side} location: ")
        fact_journey_weather_joined.show()
        fact_journey_weather_joined.createOrReplaceTempView(f"{journey_side}_fact_journey_with_weather_id")

    # Join the tables together to get both the start and end weather IDs
    fact_journey_with_weather_id = spark.sql("""
        SELECT *
        FROM start_fact_journey_with_weather_id AS fjs
        LEFT JOIN (SELECT rental_id AS end_rental_id, end_weather_id FROM end_fact_journey_with_weather_id) AS fje
            ON fjs.rental_id = fje.end_rental_id
    """).drop("end_rental_id")

    return fact_journey_with_weather_id


def setup_database(spark):
    """ Workflow for setting up BigQuery prior to main transformation steps. """

    # Create a table containing information on time
    timestamp_dimension = get_timestamp_dimension(spark)

    # Send to BigQuery with partitioning by month
    send_to_bigquery(
        timestamp_dimension, 
        additional_options = {
            "partitionField": "timestamp",
            "partitionType": "MONTH",
            "table": "dim_timestamp"
        },
        mode = "overwrite"
    )

    # Get location data from parquet, process and create bigquery table from it
    locations_dimension = get_locations_data(spark, f"gs://{GCP_GCS_BUCKET}/locations_data/livecyclehireupdates.parquet")
    send_to_bigquery(locations_dimension, {"table": "dim_locations"}, mode = "overwrite")


def transform_load_weather(spark):
    """ Workflow for transforming and loading the weather data to BigQuery. """

    # Create the weather dimension table from parquet files in the bucket
    weather_dimension = get_weather_data(
        spark,
        f"gs://{GCP_GCS_BUCKET}/weather_data/",
        MONTH_YEAR
    )

    # Update tables in BigQuery
    # If this is the first DAG run, then create the tables
    # Else, append the data to the existing tables
    write_mode = "overwrite" if MONTH_YEAR == "201612" else "append"

    # Create/update tables with partitioning by month
    send_to_bigquery(
        weather_dimension, 
        additional_options = {
            "table": "dim_weather",
            "partitionType": "MONTH",
            "partitionField": "timestamp"
        }, 
        mode = write_mode
    )


def transform_load_journeys(spark):
    """ Workflow for transforming and loading the journey data to BigQuery. """

    # Get bike usage/journey data from parquet and process
    # Each file contains data for that month (by end date), so data is split into monthly folders (YYYYMM)
    fact_journey, rental_dimension = get_usage_data(spark, f"gs://{GCP_GCS_BUCKET}/rides_data/{MONTH_YEAR}/")

    # Earliest and latest datapoint in fact_journey
    min_timestamp, max_timestamp = fact_journey.select(
                                       F.min(F.col("start_timestamp")), 
                                       F.max(F.col("end_timestamp"))
                                   ).first()

    timestamp_filter_statement = f"timestamp >= '{min_timestamp}' AND timestamp <= '{max_timestamp}'"

    # Timestamp and weather dimensions, created previously in airflow/dags/spark_transform_load.py
    # Required for joining the weather dimension to fact_journey
    timestamp_dimension = spark \
        .read.format("bigquery") \
        .option("project", GCP_PROJECT_ID) \
        .option("dataset", BIGQUERY_DATASET) \
        .option("table", "dim_timestamp") \
        .option("filter", timestamp_filter_statement) \
        .load()

    weather_dimension = spark \
        .read.format("bigquery") \
        .option("project", GCP_PROJECT_ID) \
        .option("dataset", BIGQUERY_DATASET) \
        .option("table", "dim_weather") \
        .option("filter", timestamp_filter_statement) \
        .load()

    # Create the weather dimension and add its ID as a column in fact_journey 
    fact_journey = get_weather_ids(
        spark,
        fact_journey, 
        weather_dimension,
        timestamp_dimension
    )

    # Update tables in BigQuery
    # If this is the first DAG run, then create the tables
    # Else, append the data to the existing tables
    write_mode = "overwrite" if MONTH_YEAR == "201612" else "append"

    send_to_bigquery(rental_dimension, {"table": "dim_rental"}, mode = write_mode)
    
    # Create/update tables with partitioning by month
    send_to_bigquery(
        fact_journey, 
        additional_options = {
            "table": "fact_journey",
            "partitionType": "MONTH",
            "partitionField": "end_timestamp"
        }, 
        mode = write_mode
    )


def main():
    
    # For debugging, use "local[*]" as master, for usage on GCP, use "yarn"
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName(STAGE) \
        .getOrCreate()    

    spark.sparkContext.setLogLevel("ERROR")

    # Temporary export space
    spark.conf.set("temporaryGcsBucket", GCP_GCS_BUCKET)
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    # Run the data processing stages for the specified stage 
    stages = {
        "setup_database": setup_database,
        "transform_load_weather": transform_load_weather,
        "transform_load_journeys": transform_load_journeys
    }

    stages[STAGE](spark)

    spark.stop()


if __name__ == "__main__":
    main()
