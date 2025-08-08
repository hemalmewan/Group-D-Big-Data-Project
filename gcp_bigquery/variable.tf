## define the varaibles

## define the Google Cloud credentials file
variable "google_credentials_file" {
  description = "Path to the Google Cloud credentials JSON file"
  type        = string
}

## define the Google Cloud project ID
variable "google_project_id" {
  description = "Google Cloud project ID"
  type        = string
}

## define the Google Cloud region
variable "google_region" {
  description = "Google Cloud region"
  type        = string
}

## define the BigQuery dataset name
variable "dataset_name" {
  description = "BigQuery dataset name"
  type        = string
}

## define the BigQuery table name
variable "table_name" {
  description = "BigQuery table name"
  type        = string
}
