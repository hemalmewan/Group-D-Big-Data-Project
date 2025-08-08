## define the project level IMA permissions

## define the project level IMA permissions for GCS buckets
resource "google_project_iam_member" "ds_4004_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "user:${var.ds_4004_user_email}"

}

## define the project level IMA permissions for BigQuery
resource "google_project_iam_member" "ds_4004_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "user:${var.ds_4004_user_email}"
}

## define the project level IMA permissions for bigtable
resource "google_project_iam_member" "ds_4004_bigtable_admin" {
  project = var.project_id
  role    = "roles/bigtable.admin"
  member  = "user:${var.ds_4004_user_email}"
}


## define the project level IMA permissions for compute engine
resource "google_project_iam_member" "ds_4004_composer_admin" {
  project = var.project_id
  role    = "roles/composer.admin"
  member  = "user:${var.ds_4004_user_email}"

}

## define the project level IMA permissions for service account user
resource "google_project_iam_member" "ds_4004_service_account_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "user:${var.ds_4004_user_email}"
}