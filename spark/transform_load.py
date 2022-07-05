import sys
import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from setup_database import get_column_types_from_df, send_to_bigquery


# Get arguments specifying GCS locations and pipeline parameters
GCP_PROJECT_ID = sys.argv[1]
GCP_GCS_BUCKET = sys.argv[2]
BIGQUERY_DATASET = sys.argv[3]
MONTH_YEAR = sys.argv[4]  # The month and year of the data to be processed

# Configure logger
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
curdate = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def get_usage_data(spark, file_name):
    """ 
    Extract data from the journey/usage files converted to parquet from the TfL portal. 

    The journey data has its variables transformed before being split into three tables. The
    key table is the "journey" fact table. An additional table "rental" is created, with 
    information on each bike rental (using the rental_id key). Another table, timestamp,
    is created which uses the unix timestap as a key.
    
    :param spark: A SparkSession object.

    :param file_name: The folder in which journey data is stored in parquet files.

    :return: Returns the "journey" and "rental" tables as spark dataframes.
    """

    # Read parquet
    df = spark.read.parquet(file_name) 

    # Rename columns
    old_columns = df.schema.names
    new_columns = ["rental_id", "duration", "bike_id", "end_timestamp_string", "end_station_id", "end_station_name", "start_timestamp_string", "start_station_id", "start_station_name"]

    for old_column, new_column in zip(old_columns, new_columns):
        df = df.withColumnRenamed(old_column, new_column)

    logger.debug("Loaded parquet after column renaming:")
    logger.debug("Renamed columns: " + str(df.schema.names))

    # Convert dates to timestamps and get the unix timestamp for matching with the timestamp dimension table
    for date_col in ("start", "end"):
        df = df.withColumn(f"{date_col}_timestamp", F.to_timestamp(df[f"{date_col}_timestamp_string"], "dd/MM/yyyy HH:mm")) \
               .withColumn(f"{date_col}_timestamp_id", F.unix_timestamp(f"{date_col}_timestamp").alias(f"{date_col}_timestamp_id"))

    for id_col in ("rental_id", "duration", "bike_id", "end_station_id", "start_station_id", "start_timestamp_id", "end_timestamp_id"):
        df = df.withColumn(id_col, F.col(id_col).cast("int"))

    get_column_types_from_df(df)

    # Create bike dimension table
    rental_dimension = df.select("rental_id", "bike_id", "duration").withColumnRenamed("rental_id", "id")
    logger.info("rental_dimension: " + str(rental_dimension.schema.names))

    # Create fact table
    fact_journey = df.select("rental_id", "start_station_id", "end_station_id", "start_timestamp_id", "end_timestamp_id", "start_timestamp", "end_timestamp")
    logger.info("fact_journey: " + str(rental_dimension.schema.names))

    return fact_journey, rental_dimension

def get_weather_data(spark, file_name, fact_journey, timestamp_dimension):
    """
    Extract weather data stored in parquet files relating to each location extracted in `get_locations_data`. 

    :param spark: A SparkSession object.

    :param file_name: The location of parquet files containing the weather data. Within this directory
        are three subfolders, each containing parquet files relating to a different type of weather data
        (rainfall, minimum temp, maximum temp).

    :param fact_journey: A table containing information on each journey. This is the fact table, so we will create
        a new key within this table relating it to the weather table.

    :param timestamp_dimension: A table containing a mapping of timestamp IDs (unix timestamps) to time-related variables such
        as year, month and day. Used to join the `fact_journey` and `dim_weather` tables.

    :return: Returns the weather data as a spark dataframe and a modified version of the input table `fact_journey`
        which contains a new key relating it to the weather data.
    """

    # There is a folder for each weather variable, load as temporary view
    # They are split by folders with the format YYYYMM, so get the folder for the correct (previous) month
    for weather_type in ("rainfall", "tasmin", "tasmax"):
        spark.read.parquet(f"{file_name}/{weather_type}/{MONTH_YEAR}") \
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
    weather_dimension = weather_dimension.withColumn("timestamp_id", F.unix_timestamp("time").alias("timestamp_id")) \
                                         .withColumn("location_id", F.col("location_id").cast("int")) \
                                         .withColumn("timestamp_id", F.col("timestamp_id").cast("int")) \
                                         .withColumnRenamed("time", "timestamp")
                                         
    # Create a new id column that concatenates the timestamp and location IDs
    weather_dimension = weather_dimension.withColumn("id", F.concat(weather_dimension.timestamp_id, weather_dimension.location_id)) \
                                         .withColumn("id", F.col("id").cast("int")) \
                                         .select("id", "location_id", "timestamp_id", "timestamp", "rainfall", "tasmin", "tasmax") 

    logger.info("Modified weather dataframe: ")
    print(weather_dimension.show())
    get_column_types_from_df(weather_dimension)

    # Get a version of the timestamp dimension table with the required columns for making the joins
    timestamp_dimension.select("id", "year", "month", "dayofmonth") \
            .withColumnRenamed("id", "timestamp_id") \
            .createOrReplaceTempView("timestamp_dimension_to_join")

    # Join weather data to time for joining with the fact table
    weather_dimension.withColumnRenamed("id", "weather_id") \
        .createOrReplaceTempView("weather_dimension_to_join")
    
    spark.sql("""
        SELECT *
        FROM weather_dimension_to_join AS wdj
        LEFT JOIN timestamp_dimension_to_join AS tdj
            ON wdj.timestamp_id = tdj.timestamp_id
    """) \
        .createOrReplaceTempView("weather_dimension_time")

    logger.info("Weather data joined to time: ")
    spark.sql("SELECT * FROM weather_dimension_time LIMIT 10").show()

    # Below we create an ID mapping the fact table to the weather dimension
    # There are two locations/times for each journey (start and end)
    # So, we create a key for both the start and the end mapping each to the correct weather

    fact_cols_to_keep = ["rental_id", "start_station_id", "end_station_id", "start_timestamp_id", "end_timestamp_id", "start_timestamp", "end_timestamp"]
    
    # Setup separate fact tables for the start and end IDs, to be rejoined later
    fact_journey.createOrReplaceTempView("start_fact_journey")
    fact_journey.createOrReplaceTempView("end_fact_journey")

    for journey_side in ("start", "end"):

        logger.info(f"Getting weather ID for {journey_side} location")

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
            INNER JOIN weather_dimension_time as wdt
                ON fjtj.year = wdt.year 
                AND fjtj.month = wdt.month 
                AND fjtj.dayofmonth = wdt.dayofmonth
                AND fjtj.{journey_side}_station_id = wdt.location_id
        """)

        fact_journey_weather_joined = fact_journey_weather_joined.withColumnRenamed("weather_id", f"{journey_side}_weather_id")

        logger.info(f"{journey_side} journey data joined to the weather dimension: ")
        fact_journey_weather_joined.show()
 
        # Keep the new column relating to the weather dimension
        fact_journey_weather_joined = fact_journey_weather_joined.select(*(fact_cols_to_keep + [f"{journey_side}_weather_id"]))
        
        logger.info(f"Fact table joined to weather by {journey_side} location: ")
        fact_journey_weather_joined.show()
        fact_journey_weather_joined.createOrReplaceTempView(f"{journey_side}_fact_journey_with_weather_id")

    fact_journey_with_weather_id = spark.sql("""
        SELECT *
        FROM start_fact_journey_with_weather_id AS fjs
        LEFT JOIN (SELECT rental_id AS end_rental_id, end_weather_id FROM end_fact_journey_with_weather_id) AS fje
            ON fjs.rental_id = fje.end_rental_id
    """).drop("end_rental_id")

    logger.info("Final joined fact_journey: ")
    fact_journey_with_weather_id.show()

    return fact_journey_with_weather_id, weather_dimension

def main():
    
    # For debugging, use "local[*]" as master, for usage on GCP, use "yarn"
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("transform_load") \
        .getOrCreate()    

    spark.sparkContext.setLogLevel("ERROR")

    # Temporary export space
    spark.conf.set("temporaryGcsBucket", GCP_GCS_BUCKET)

    # Get bike usage/journey data from parquet and process
    # Each file contains data for that month (by end date), so data is split into monthly folders (YYYYMM)
    fact_journey, rental_dimension  = get_usage_data(spark, f"gs://{GCP_GCS_BUCKET}/rides_data/{MONTH_YEAR}/")

    # Timestamp dimension, created previously in airflow/dags/spark_transform_load.py
    # Required for joining the weather dimension to fact_journey
    timestamp_dimension = spark.read.format("bigquery") \
        .option("project", GCP_PROJECT_ID) \
        .option("dataset", BIGQUERY_DATASET) \
        .option("table", "dim_timestamp")

    # Create the weather dimension and add its ID as a column in fact_journey 
    fact_journey, weather_dimension = get_weather_data(
        spark,
        f"gs://{GCP_GCS_BUCKET}/weather_data/", 
        fact_journey, 
        timestamp_dimension
    )

    # Update tables in BigQuery
    send_to_bigquery(fact_journey, additional_options = {"table": "fact_journey"}, mode = "append")
    send_to_bigquery(weather_dimension, additional_options = {"table": "dim_weather"}, mode = "append")
    send_to_bigquery(rental_dimension, {"table": "dim_rental"}, mode = "append")

    spark.stop()


if __name__ == "__main__":
    main()
