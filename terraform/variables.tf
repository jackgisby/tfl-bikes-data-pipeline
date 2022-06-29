locals {
  data_lake_bucket = "bikes_data_lake"
}

variable "project" {
  description = "Bike project ID"
  default = "de-camp-353016"
  type = string
}

variable "region" {
  description = "GCP resource region"
  default = "europe-north1"
  type = string
}

variable "storage_class" {
  description = "Standard storage has no minimum storage duration"
  default = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that transformed bike rental data will be loaded to"
  type = string
  default = "bikes_data_warehouse"
}
