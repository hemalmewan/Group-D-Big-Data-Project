## define the variables

## define the Google Cloud credentials file
variable "google_credentials_file" {
  description = "Path to the Google Cloud credentials file"
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

## define the Composer environment name
variable "composer_environment_name" {
  description = "Composer environment name"
  type        = string
}