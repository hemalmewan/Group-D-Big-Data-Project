## define the provider
provider "google" {
  credentials = file(var.google_credentials_file)
  project     = var.google_project_id
  region      = var.google_region
}

## define the google cloud composer environment
resource "google_composer_environment" "ds_4004" {
  name    = var.composer_environment_name
  project = var.google_project_id
  region  = var.google_region
  config {

    software_config {
      image_version = "composer-3-airflow-2"
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      triggerer {
        cpu       = 0.5
        memory_gb = 1
        count     = 1
      }
      dag_processor {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }

    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      ##define the existing service account
      service_account = "ds-4004-group-d-project@ds-4004-d-big-data-project.iam.gserviceaccount.com"
    }
  }
}

