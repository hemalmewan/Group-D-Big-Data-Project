## define the provider
provider "google" {
  credentials = file(var.google_credentials_file)
  project     = var.google_project_id
  region      = var.google_region

}

## define google bigquery dataset
resource "google_bigquery_dataset" "ds_4004" {
  dataset_id  = var.dataset_name
  description = "Dataset for the 4004 project"
  location    = var.google_region
  labels = {
    environment = "production"
    project     = "4004"
  }

}

## define google bigquery table
resource "google_bigquery_table" "ds_4004_table" {
  dataset_id  = google_bigquery_dataset.ds_4004.dataset_id
  table_id    = var.table_name
  description = "Big Query Table for the 4004 project"
  labels = {
    environment = "production"
    project     = "4004"
  }

}
