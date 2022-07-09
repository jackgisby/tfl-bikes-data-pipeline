Setup
=====

# Setup Google Cloud Platform

This document describes the basics of creating a Google Cloud Platform (GCP) account, setting up a project and creating a service account to be used by the pipeline. 

## Creating a GCP account

[Creating a GCP account](https://cloud.google.com/apigee/docs/hybrid/v1.1/precog-gcpaccount) is simple and free. It uses an existing google/gmail account. Note that there is both the option of the [free tier and a free trial](https://cloud.google.com/free). The free tier is permanent, and gives you access to a certain amount of processing for some of the main GCP services. This includes a generous amount of processing using BigQuery, which this pipeline uses to create the main database. 

The second option is the free trial. This includes $300 in free credits that is available for 90 days after activation. The data processed by this pipeline is not huge, so can be run for very little to no cost even without the free trial. In the process of creating this pipeline, I used £22 worth of free trial credit. Since this included many development iterations and lots of debugging, it is likely that running the pipeline a single time would cost very little. The main cost came from the Dataproc cluster that hosts spark jobs for transforming the data and loading it to BigQuery.

## Creating a GCP project

Next, you must [create a google cloud project](https://cloud.google.com/apigee/docs/hybrid/v1.1/precog-gcpproject). You could call it something along the lines of "bikes-data-pipeline". 

Next, you must enable [relevant APIs](https://console.cloud.google.com/apis/library?project=de-camp-353016) for the project, including that for Cloud Storage, Compute Engine (if you wish to run the pipeline on a VM), BigQuery and Dataproc.

Finally, we need to create service accounts that will be authorised to use these APIs. In IAM -> Service accounts, add a service account with a "Viewer" role. You can create keys for this service account at Actions -> Manage Keys -> AddKey -> Create new key -> Create. The key must be saved to your local computer or VM (e.g. in `~/.google/credentials/`), depending on where you want to run the pipeline from. 

This service account needs the roles that will allow it to interact with GCP storage, BigQuery and Dataproc. Go to IAM & Admin -> IAM and click to edit the service account. It will need the following roles:
- Viewer
- Service Account User
- Storage Admin
- Storage Object Admin
- BigQuery Admin
- Dataproc Admin

# Setup environment

The next step is to set up the environment from which Airflow will orchestrate the pipeline. I suggest either running this on your local machine or using a virtual machine (VM) on GCP. Note that the docker environment for Airflow uses a lot of memory, so if you exceed the RAM on your local machine you may need to switch to a VM instance. The instructions in this document will apply to both running airflow locally and running it on the VM, but I will not provide a full explanation of creating the VM instance.

## Install required packages

On your local machine or VM, you must install Docker, Docker Compose and Terraform. The remaining dependencies will be installed within the container we will create for Airflow. 

Of course, you must also download this repository, like so:
```
git clone https://github.com/jackgisby/tfl-bikes-data-pipeline.git
cd tfl-bikes-data-pipeline
```

## Setup environment variables

In the top-level directory of this repository, you will see the file `.env_example`. This should be filled in and renamed to `.env`. You must add the details of your google cloud project, including the name of the project, storage bucket and BigQuery dataset. These are also needed for Terraform (see `terraform/variables.tf`).

For digesting the weather data, you will also need to register an account with the CEDA archive, where the data is hosted. You will be able to create an FTP password that will give you access to the relevant data. The FTP password and username must be put in the `.env` file before building the docker environment.

## Setup the cloud resources

In the `terraform/` directory, there is a script to setup the cloud resources we will need for the project. The following Terraform commands must be run from the base directory:

```
cd terraform
terraform init
terraform plan
terraform apply
```

Then answer "yes" after running terraform apply. You can also destroy the created resources using `terraform destroy`. But be careful, this may result in lost data!

## Using Docker Compose

This repository contains a modified version of [the quick-start Docker Compose Airflow installation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) and a Dockerfile that additionally installs the GCS SDK and additional Python dependencies on top of the Airflow base image. Note that the quick-start Docker Compose has a lot of extra functionality that we don't use, and ideally would be reduced to host only the components that we need. Regardless, you can build and activate the Docker images with the following commands:

```
docker-compose build
docker-compose up airflow-init
docker-compose up
```

When you're done with the environment, you can run `docker-compose down` to take down the images.
