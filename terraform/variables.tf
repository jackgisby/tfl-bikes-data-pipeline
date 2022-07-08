locals {
  data_lake_bucket = ""
}

variable "project" {
  description = "Bike project ID"
  type = string
}

variable "region" {
  description = "GCP resource region"
  type = string
}

variable "storage_class" {
  description = "Standard storage has no minimum storage duration"
  default = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that transformed bike rental data will be loaded to"
  type = string
}
