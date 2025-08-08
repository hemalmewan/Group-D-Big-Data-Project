## provider configuration
provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}


## create the google cloud storage bucket
resource "google_storage_bucket" "ds_4004_d" {
  name          = var.bucket_name
  location      = var.bucket_location
  force_destroy = true
}