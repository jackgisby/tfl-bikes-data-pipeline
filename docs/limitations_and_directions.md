Limitations and future directions
=================================

The development time of this pipeline was limited by the duration of the GCP free trial. The scope of the project was restricted so that it could reasonably be completed during this trial, given that only a few hours each week could be devoted to development of the pipeline. The project is complete in the sense that it is fully functional: it can successfully ingest data from multiple sources, integrate them and append them to the primary database on a monthly schedule. However, there are a number of methods that could be improved and some limitations that could be avoided. Some of these are discussed below.

# Scheduling

Airflow does a lot of the heavy lifting in this project. After setting up a basic GCP project and running terraform, Airflow manages all the pipeline tasks, including infrastructure management (e.g. creating/removing Dataproc clusters, creating BigQuery datasets/tables), basic transformations and submission of spark jobs. However, there are some drawbacks to the Airflow approach used by this project.

## Managed infrastructure

GCP has a Cloud Composer service, which allows you to manage Airflow from within google cloud and has integration with relevant products such as BigQuery, GCS and Dataproc. Currently, this project uses Docker to run Airflow either locally or on a virtual machine. It would be more convenient and sustainable to run Airflow using Cloud Composer. Otherwise, the local or virtual machine will have to keep running, and must not be used to run other tasks lest they interfere with the job scheduling. Cloud Composer can be used to automatically provision resources for the pipelines. As we will discuss later in this document, further integration of continous integration practices would be useful for the deployment of this project. Cloud Composer allows you to build, deploy and test Airflow workflows.

## Separation of scheduling and data transformation

Ideally, all data processing should be separated from the orchestration by Airflow. While a PySpark job is submitted to Dataproc to do most of the heavy lifting, a small amount of data transformation is done as part of the ingestion stage within the Docker container. This can interfere with the Airflow scheduling and should ideally be submitted to be run elsewhere on GCP. This would be easier to implement with Cloud Composer, but could feasibly be done without it. 

Alternatively, we could move all the data processing to the PySpark job, which is processed separately to the Airflow instance. We do some data processing prior to upload in order to remove unnecessary data and reduce the GCS storage used by the project. If we uploaded the entire datasets to GCS, then Spark could do the filtering and there would be greater separation of scheduling and data processing.

# Continous integration and testing

## Continous integration

Currently, the project requires some manual setup, including the creation of a GCP project, running Terraform to create infrastructure and activating the Docker containers in order to start the Airflow DAGs. In order to make changes to the project, you may need to re-start the Airflow instance and possibly make changes manually on GCP. Ideally, all of these steps would be automated along with testing and data quality checks. Some of these problems could be improved by using Cloud Composer to host Airflow, such that the DAGs can be automatically be deployed, tested and run on GCP. We could also use a service like GitHub Actions to automate the creation of GCP resources and the activation of DAGs on a virtual machine. 

## Quality checks and testing

Automated unit and integration testing would be useful for testing DAGs and the spark pipeline to ensure that updates to the workflow do not break existing functionality. Additionally, including extensive data quality checks/validation would have been useful. While there are some in-built checks within the pipeline, these do not encompass the entirety of the dataset. If data is not the expected type, it is likely to be picked up by the PySpark script, but lack of specific testing might make problems at this stage difficult to debug. 

## Logging

There is already some logging integrated into the pipeline, although in some areas it is sparse. In certain DAGs and in the PySpark script, logging is employed to check the state of variables and identify how data has changed after transformation. However, the presence of logging is largely a function of the amount of time that was spent debugging that particular section. Ideally, the logging would be improved such that it covers a greater proportion of the pipeline and includes more information.

# Other

## Spark script

In its current iteration, the Spark script is quite lengthy, containing three modes of operation and a number of shared functions. It would be better to split this script up such that there is a script for each pipeline mode, and a script containing the shared functions for the workflows to import. The script has been created as a single file as this was the simplest way to automatically upload and run the job on Dataproc. If we were to incorporate continous development practices into this pipeline, we could automatically deploy the spark scripts to GCS so that they are ready to use when required by the scheduler, as opposed to uploading the current file to GCS as part of the DAGs.

## Documentation

We currently have two forms of documentation:
- Within scripts, there are docstrings for Python functions and comments describing the purpose and methods of the code. 
- Within `docs/` there is a holistic guide to the pipeline and how it may be run.

Again, if we were to incorporate continous integration methodologies into the pipeline, we could potentially automate the generation of a cohesive document describing the pipeline and its individual components.

## Weather data

The spatial-resolution of the weather data may be more than is really necessary for the pipeline. It is unlikely that the weather at one end of a journey will be significantly different from that at the other. It might have been more efficient to collect the daily weather for the london average rather than including variables for every single cycle hub. 

## Complexity

The pipeline is quite complex considering the complexity of the problem being solved. I was interested in learning about Google Cloud and Airflow/Spark, so wished to include them. For instance, the pipeline could have been run without the cloud, using a local machine with high RAM. In this case, PostgreSQL could be used rather than BigQuery and spark/python jobs could have been scheduled to run locally rather than via Google Dataproc.
