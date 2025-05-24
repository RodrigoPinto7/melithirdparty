# Infraestructura para el Data Challenge de Meli usando Terraform

 terraform {
   backend "gcs" {
     bucket = "terraform-state-melithirdparty"
     prefix = "terraform/state"
   }
 }

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  default = "melithirdparty"
}

variable "region" {
  default = "us-central1"
}

# Bucket para el estado de Terraform
resource "google_storage_bucket" "terraform_state" {
  name          = "terraform-state-melithirdparty"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }
}

# Crear bucket de GCS para los datos
resource "google_storage_bucket" "aws_billing_bucket" {
  name     = "aws-billing-data-melithirdparty"
  location = var.region
  uniform_bucket_level_access = true
}

# Crear dataset de BigQuery
resource "google_bigquery_dataset" "billing_dataset" {
  dataset_id                  = "aws_billing"
  location                   = var.region
  delete_contents_on_destroy = true
}

# Crear entorno de Cloud Composer (Airflow)
# Comentado para evitar cargos innecesarios en la etapa inicial de desarrollo
# Descomentar cuando se quiera desplegar Composer
/*
resource "google_composer_environment" "composer_env" {
  name   = "aws-billing-pipeline"
  region = var.region

  config {
    node_count = 3

    software_config {
      image_version   = "composer-2.2.3-airflow-2.5.1"
      python_version  = "3"
      env_variables = {
        PROJECT_ID = var.project_id
        REGION      = var.region
        DATASET_ID  = google_bigquery_dataset.billing_dataset.dataset_id
        BUCKET_NAME = google_storage_bucket.aws_billing_bucket.name
      }
    }
  }
}
*/

output "bucket_name" {
  value = google_storage_bucket.aws_billing_bucket.name
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.billing_dataset.dataset_id
}

# output "composer_environment" {
#   value = google_composer_environment.composer_env.name
# }
