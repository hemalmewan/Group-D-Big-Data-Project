## define the variables

## define file credentials for Google Cloud
variable "credentials_file" {
  description = "Path to the Google Cloud credentials file"
  type        = string

}

## define the project ID
variable "project_id" {
  description = "Google Cloud project ID"
  type        = string

}

## define the region
variable "region" {
  description = "Google Cloud region"
  type        = string

}

## define the bucket name
variable "bucket_name" {
  description = "Name of the Google Cloud Storage bucket"
  type        = string
}

## define the bucket location
variable "bucket_location" {
  description = "Location of the Google Cloud Storage bucket"
  type        = string
}
