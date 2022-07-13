Data transformation
===================

In this document, the data transformation and loading steps are discussed. These steps are carried out with Spark, run on a Dataproc cluster. 

## Automatic management of infrastructure

In the DAG script (`spark_transform_load.py`), there is a boolean variable `CREATE_INFRASTRUCTURE`. If the value of this variable is set to `True`, then the DAGs include a step to create the Dataproc cluster; the Spark job is then submitted, and after it completes the cluster is destroyed. This is fine if you are running a single DAG, for instance you just want to run the transform-load steps for the previous month's data. But, if you want to run this step for many months at a time, repeatedly creating and destroying the infrastructure will be very inefficient. In this case, `CREATE_INFRASTRUCTURE` should be set to `False` and the Dataproc cluster should be created prior to running the DAGs. 

The cluster teardown task is given the trigger rule "all_done", meaning that even if a previous task fails the cluster will still be deleted before the DAG resolves. 

## BigQuery setup

Before the transformation steps, the BigQuery dataset and tables must be created. This is achieved using a DAG, but it only needs to run a single time. We create empty tables for the journey, locations and weather data, which will be populated using Spark in the transform-load DAGs. The schema for these tables are stored in `assets/schema/*.json` where `*` refers to "fact_schema" for the journey data, `rental_schema` for the rental table (a derivative of the journey data) and `weather_schema` for the weather data.

Both the setup and transform-load DAGs upload the PySpark script to the GCS as part of the workflow. Each of the DAGs submit this script as a Dataproc job, and they pass through an argument referring to the mode in which to run the script. In this case, we run the PySpark script in "setup_database" mode. In this instance, spark creates a "timestamp" dimension, containing variables relating to time. The ID of this table relates to the `timestamp_id` columns in the journeys table. The DAG also transforms the cycle stations data. Then, both tables are loaded to BigQuery.

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/dags/setup_bigquery.png?raw=true" />
</p>

## Transform-load DAGs

Now that BigQuery is prepared, Spark can be used to transform and load the main datasets. The function `create_transform_dag` is used to create two nearly identical DAGs, one that processes the weather data and another that transforms the journey data. The key difference between the DAGs is the parameter that is passed to the PySpark job, which dicates the steps to be run by Spark. The month and year of the data to be processed is also provided to PySpark, which dicates the data to be transformed and uploaded to BigQuery. The DAG is very simple, especially if the infrastructure-related variables are not included.  

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/dags/transform_load_journeys.png?raw=true" />
</p>

The PySpark steps for the weather transformation are relatively simple. The function `get_weather_data` is used to load the weather data for a particular month, before the RDD is uploaded to BigQuery. The processing involves loading each of the weather datasets (rainfall, tasmin, tasmax) and combining them into a single table with joins. The parquet columns are cast to the correct type and a unique ID is created for each data observation, based on the time and location. 

The cycle journey data requires more extensive processing. The function `get_usage_data` is used to read the data for the current month as an RDD. The columns are renamed, the columns are cast to the correct types and the table is split into a fact-like (or "fact-less") table and a smaller dimension table containing additional information for each rental. The Spark workflow then loads the timestamp and weather tables from BigQuery. The fact-like table does not have an ID that corresponds to the correct weather observation, so we need to create one. It would probably be better to perform some calculations in Spark to create the weather ID in the fact-like table without reading and joining the weather data; a drawback of this alternative approach would be that, if there was an observation missing in the weather data, we would create an ID for it that does not map to any rows in the weather table.

Regardless, in the current implementation we join the timestamp dimension table to both the weather table and the fact-like journeys table. The weather data is obtained daily, while the journeys are measured to the minute. Therefore, the timestamp dimension is helpful because we can easily join the weather data to the fact-like table by the "year", "month" and "monthofyear" columns. We perform this join twice: i) once for the time and location of the start of the journey; and ii) once again for the end destination of the journey. Therefore, we can create a weather ID for both the start and end of the journey.

## Final BigQuery structure

Once the DAGs are activated, the BigQuery tables will be created and populated, and further data will be appended to the tables each month as new data is ingested. The tables follow the structure below, where journeys is a fact-like (or "fact-less") table, because it performs the function of a fact table in a star schema, but does not contain any measures that can be aggregated. All measures are contained in dimension tables. We have moved journey duration to the rental table, but we could have kept this value in the primary journey table, making it a proper fact table. But, duration is not of primary interest, so it was moved. 

As discussed in the README, the database is intended to be used like a star schema, where tables are joined to the fact-like journeys table to perform various analyses. This is how the dashboards were created (see an example of joins being performed in `sql/make_view.sql` and some example aggregation queries in `sql/aggregations.sql`). There is also a natural relationship between the weather table and the timestamp table, and the weather table with the locations table. If one was primarily interested in weather, the locations and timestamp tables could be joined to the weather table in order to perform aggregations and analyses. However, this was not the intention of the schema design; in general, joins between tables other than "journeys" should be avoided.

<p align="center">
  <img src="https://github.com/jackgisby/tfl-bikes-data-pipeline/blob/main/assets/main_relations.drawio.png?raw=true" />
</p>
