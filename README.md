Transport for London Santander Cycles Data Pipeline
===================================================

# About the data

# About the pipeline

## Processing stages

## Database

## Dashboard

# Running the pipeline

- Create gcloud account, project, and user with appropriate permissions
- Install terraform, gcloud CDK, docker, docker-compose onto local or virtual machine
- Add gcloud credentials (~/.google/credentials/google_credentials.json) and as environment variable, then authorise
- Terraform
- Airflow

# License

The data processing pipeline is licensed under the GNU General Public License v3.0 (see [LICENSE file](https://github.com/jackgisby/tfl_bikes_data_pipeline/blob/main/LICENSE) for licensing information).

The bike rental data is [powered by TfL open data](https://tfl.gov.uk/corporate/terms-and-conditions/transport-data-service). These data contain Ordnance Survery-derived data (© Crown copyright and database rights 2016) and Geomni UK Map data (© and database rights [2019]).

Weather data was obtained from the [CEDA Archive's HADUK-Grid regional climate observations](https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb), licensed under the [Open Government Licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
