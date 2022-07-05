import sys
import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


# Get arguments
GCP_PROJECT_ID = sys.argv[1]
GCP_GCS_BUCKET = sys.argv[2]
BIGQUERY_DATASET = sys.argv[3]

# Configure logger
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)
curdate = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

def get_column_types_from_df(df):
    """
    Prints the column types of a spark dataframe to logger.

    :param df: A spark dataframe.
    """

    for column_name in df.schema.names:
        logger.info(f"{column_name}: {df.schema[column_name].dataType}")

def get_usage_data(spark, file_name):
    """ 
    Extract data from the journey/usage files converted to parquet from the TfL portal. 

    The journey data has its variables transformed before being split into three tables. The
    key table is the "journey" fact table. An additional table "rental" is created, with 
    information on each bike rental (using the rental_id key). Another table, timestamp,
    is created which uses the unix timestap as a key.
    
    :param spark: A SparkSession object.

    :param file_name: The folder in which journey data is stored in parquet files.

    :return: Returns "journey", "rental" and "timestamp" tables as spark dataframes.
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

    # Create date dimension table
    begin_timestamp, end_timestamp = "2017-01-01 00:00", "2021-12-31 23:59"

    # Creates a timestamp dimension with the unix_timestamp as the key
    timestamp_dimension = spark.createDataFrame([(1,)], ["c"]) \
        .withColumn("timestamp", F.explode(F.expr(f"sequence(to_timestamp('{begin_timestamp}'), to_timestamp('{end_timestamp}'), interval 1 minute)"))) \
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
    timestamp_dimension.show()

    get_column_types_from_df(timestamp_dimension)

    # We have some data outside the years of interest, so remove
    df = df.filter(df["end_timestamp"] >= F.lit(begin_timestamp)) \
           .filter(df["end_timestamp"] <= F.lit(end_timestamp))

    # Create bike dimension table
    rental_dimension = df.select("rental_id", "bike_id", "duration").withColumnRenamed("rental_id", "id")
    logger.info("rental_dimension: " + str(rental_dimension.schema.names))

    # Create fact table
    fact_journey = df.select("rental_id", "start_station_id", "end_station_id", "start_timestamp_id", "end_timestamp_id", "start_timestamp", "end_timestamp")
    logger.info("fact_journey: " + str(rental_dimension.schema.names))

    return fact_journey, rental_dimension, timestamp_dimension

def get_locations_data(spark, file_name):
    """ 
    Extract data from the journey/usage files converted to parquet from the TfL portal. 

    :param spark: A SparkSession object.

    :param file_name: The location of a parquet file containing the journey data.

    :return: Returns the locations data as a spark dataframe.
    """

    df = spark.read.parquet(file_name) \
        .withColumnRenamed("terminalName", "terminal_name")

    logger.info("Raw imported locations data:")
    df.show()

    get_column_types_from_df(df)

    for id_col in ("id", "terminal_name"):
        df = df.withColumn(id_col, F.col(id_col).cast("int"))

    # Store latitude and longitude as custom decimal types
    df = df.withColumn("lat", F.col("lat").cast("decimal(8, 6)")) \
           .withColumn("long", F.col("long").cast("decimal(9, 6)"))

    logger.info("Reformatted imported locations data:")
    df.show()

    return df

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
    for weather_type in ("rainfall", "tasmin", "tasmax"):
        spark.read.parquet(f"{file_name}/{weather_type}/").createOrReplaceTempView(f"{weather_type}_from_parquet")

    # Combine the views together into a weather dimension table
    weather_dimension = spark.sql("""
        SELECT rainfall.location_id, rainfall.time, rainfall.rainfall, tasmin.tasmin, tasmax.tasmax
        FROM rainfall_from_parquet AS rainfall
        LEFT JOIN tasmin_from_parquet AS tasmin
            ON rainfall.location_id = tasmin.location_id AND rainfall.time = tasmin.time
        LEFT JOIN tasmax_from_parquet AS tasmax
            ON rainfall.location_id = tasmax.location_id AND rainfall.time = tasmax.time
    """)

    # Transform the weather data
    weather_dimension = weather_dimension.withColumn("id", F.monotonically_increasing_id()) \
                                         .withColumn("timestamp_id", F.unix_timestamp("time").alias("timestamp_id")) \
                                         .withColumn("id", F.col("id").cast("int")) \
                                         .withColumn("location_id", F.col("location_id").cast("int")) \
                                         .withColumn("timestamp_id", F.col("timestamp_id").cast("int")) \
                                         .withColumnRenamed("time", "timestamp") \
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

def send_to_bigquery(df, additional_options=None):
    
    # Append data to pre-existing BigQuery table
    df = df.write.format("bigquery") \
        .mode("overwrite") \
        .option("project", GCP_PROJECT_ID) \
        .option("dataset", BIGQUERY_DATASET) 

    for option_name, option_value in additional_options.items():
        df = df.option(option_name, option_value)

    df = df.save()

def main():
    
    # For debugging, use "local[*]" as master, for usage on GCP, use "yarn"
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("transform_load_bike_data") \
        .getOrCreate()    

    spark.sparkContext.setLogLevel("ERROR")

    # Temporary export space
    spark.conf.set("temporaryGcsBucket", GCP_GCS_BUCKET)

    # Get data from parquet and process
    fact_journey, rental_dimension, timestamp_dimension = get_usage_data(spark, f"gs://{GCP_GCS_BUCKET}/rides_data/")

    fact_journey, weather_dimension = get_weather_data(spark, f"gs://{GCP_GCS_BUCKET}/weather_data/", fact_journey, timestamp_dimension)

    send_to_bigquery(
        fact_journey, 
        additional_options = {
            "partitionField": "end_timestamp",
            "partitionType": "MONTH",
            "table": "fact_journey"
        }
    )

    send_to_bigquery(
        timestamp_dimension, 
        additional_options = {
            "partitionField": "timestamp",
            "partitionType": "MONTH",
            "table": "dim_timestamp"
        }
    )

    send_to_bigquery(
        weather_dimension, 
        additional_options = {
            # "partitionField": "timestamp",
            # "partitionType": "MONTH",
            "table": "dim_weather"
        }
    )

    send_to_bigquery(rental_dimension, {"table": "dim_rental"})

    # Get data from parquet and process
    locations_dimension = get_locations_data(spark, f"gs://{GCP_GCS_BUCKET}/locations_data/livecyclehireupdates.parquet")
    send_to_bigquery(locations_dimension, {"table": "dim_locations"})
    spark.stop()


if __name__ == "__main__":
    main()
