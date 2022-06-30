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
    for column_name in df.schema.names:
        logger.info(f"{column_name}: {df.schema[column_name].dataType}")

def get_usage_data(spark, file_name):
    """ Extract data from the journey/usage files converted to parquet from the TfL portal. """

    # Read parquet
    df = spark.read.parquet(file_name) 

    logger.info("Raw imported usage data:")
    df.show()

    # Rename columns
    old_columns = df.schema.names
    new_columns = ["rental_id", "duration", "bike_id", "end_timestamp_string", "end_station_id", "end_station_name", "start_timestamp_string", "start_station_id", "start_station_name"]

    for old_column, new_column in zip(old_columns, new_columns):
        df = df.withColumnRenamed(old_column, new_column)

    logger.debug("Renamed columns:")
    df.show()

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
    logger.debug("Timestamp dimension:")
    timestamp_dimension.show()

    get_column_types_from_df(timestamp_dimension)

    # We have some data outside the years of interest, so remove
    df = df.filter(df["end_timestamp"] >= F.lit(begin_timestamp)) \
           .filter(df["end_timestamp"] <= F.lit(end_timestamp))

    # Create bike dimension table
    rental_dimension = df.select("rental_id", "bike_id", "duration").withColumnRenamed("rental_id", "id")
    logger.info("rental_dimension:")
    rental_dimension.show()

    # Create fact table
    fact_table = df.select("rental_id", "start_station_id", "end_station_id", "start_timestamp_id", "end_timestamp_id", "start_timestamp", "end_timestamp")
    logger.info("fact_table:")
    fact_table.show()

    return fact_table, rental_dimension, timestamp_dimension

def get_locations_data(spark, file_name):
    """ Extract data from the journey/usage files converted to parquet from the TfL portal. """

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

def send_to_bigquery(df, additional_options=None):
    
    # Append data to pre-existing BigQuery table
    df = df.write.format("bigquery") \
        .option("project", GCP_PROJECT_ID) \
        .option("dataset", BIGQUERY_DATASET) 

    for option_name, option_value in additional_options.items():
        df = df.option(option_name, option_value)

    df = df.save()

def main():
    
    # For debugging, use "local[*]" as master
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .appName("transform_load_bike_data") \
        .getOrCreate()    

    spark.sparkContext.setLogLevel("ERROR")

    # Temporary export space
    spark.conf.set("temporaryGcsBucket", GCP_GCS_BUCKET)

    # # Get data from parquet and process
    # fact_table, rental_dimension, timestamp_dimension = get_usage_data(spark, f"gs://{GCP_GCS_BUCKET}/rides_data/")

    # send_to_bigquery(
    #     spark, 
    #     fact_table, 
    #     additional_options = {
    #         "partitionField": "end_timestamp",
    #         "partitionType": "MONTH",
    #         "table": "fact_table"
    #     }
    # )

    # send_to_bigquery(
    #     timestamp_dimension, 
    #     additional_options = {
    #         "partitionField": "timestamp",
    #         "partitionType": "MONTH",
    #         "table": "dim_timestamp"
    #     }
    # )

    # send_to_bigquery(rental_dimension, {"table": "dim_rental"})

    # Get data from parquet and process
    locations_dimension = get_locations_data(spark, f"gs://{GCP_GCS_BUCKET}/locations_data/livecyclehireupdates.parquet")
    send_to_bigquery(locations_dimension, {"table": "dim_locations"})

    spark.stop()


if __name__ == "__main__":
    main()
