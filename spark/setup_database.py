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

def get_timestamp_dimension(spark):
    """ 
    Creates a table mapping timestamp IDs (unix timestamp) to other time-related variables,
    including timestamp, year, month, day of month, week of year, hour and minute.

    :param spark: A SparkSession object.

    :return: A Spark RDD containing the table.
    """

    # Create date dimension table
    begin_timestamp, end_timestamp = "2016-01-01 00:00", "2022-01-31 23:59"

    # Creates a timestamp dimension with the unix_timestamp as the key
    df = spark.createDataFrame([(1,)], ["c"]) \
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

def send_to_bigquery(df, additional_options=None, mode="append"):
    
    # Append data to pre-existing BigQuery table
    df = df.write.format("bigquery") \
        .mode(mode) \
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
        .appName("setup_database") \
        .getOrCreate()    

    spark.sparkContext.setLogLevel("ERROR")

    # Temporary export space
    spark.conf.set("temporaryGcsBucket", GCP_GCS_BUCKET)

    # Get data from parquet and process
    timestamp_dimension = get_timestamp_dimension(spark, f"gs://{GCP_GCS_BUCKET}/rides_data/")

    # Create table with partitioning by month
    send_to_bigquery(
        timestamp_dimension, 
        additional_options = {
            "partitionField": "timestamp",
            "partitionType": "MONTH",
            "table": "dim_timestamp"
        },
        mode = "overwrite"
    )

    # Get data from parquet, process and create bigquery table from it
    locations_dimension = get_locations_data(spark, f"gs://{GCP_GCS_BUCKET}/locations_data/livecyclehireupdates.parquet")
    send_to_bigquery(locations_dimension, {"table": "dim_locations"}, mode = "overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
