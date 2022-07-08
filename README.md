Transport for London Santander Cycles Data Pipeline
===================================================

# An overview of the pipeline

## Processing stages

## Database

## Dashboard

# Pipeline documentation

For a more detailed explanation of the pipeline and instructions on how to setup the pipeline yourself, refer to the following documents:

1. [Setup a google cloud project](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/1_setup_gcp.md)
2. [Setup a local or virtual machine](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/2_setup_environment.md)
3. [Create google cloud resources](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/3_create_gcp_resources.md)
4. [Data ingestion](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/4_data_ingestion.md)
5. [Data transformation](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/5_data_transformation.md)
6. [Data visualisation](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/6_data_visualisation.md)

We also provide a more detailed [description of the datasets](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/data_sources.md) processed by the pipeline, further information on how the [integrated data is stored in BigQuery](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/integrated_database.md) and a discussion of the [limitations and future directions](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/limitations_and_directions.md) of the pipeline.

# License

The data processing pipeline is licensed under the GNU General Public License v3.0 (see [LICENSE file](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/LICENSE) for licensing information).

The bike rental data is [powered by TfL open data](https://tfl.gov.uk/corporate/terms-and-conditions/transport-data-service). These data contain Ordnance Survery-derived data (© Crown copyright and database rights 2016) and Geomni UK Map data (© and database rights [2019]).

Weather data was obtained from the [CEDA Archive's HADUK-Grid regional climate observations](https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb), licensed under the [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
