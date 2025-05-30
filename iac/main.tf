terraform {
  backend "gcs" {
    bucket = "terraform-state-melithirdparty-460619"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# IAM Module
module "iam" {
  source = "./modules/iam"

  project_id = var.project_id
}

# GCS Module
module "gcs" {
  source = "./modules/gcs"

  project_id                = var.project_id
  region                    = var.region
  raw_bucket               = var.raw_bucket
  stage_bucket             = var.stage_bucket
  raw_bucket_retention_days = var.raw_bucket_retention_days
  stage_bucket_retention_days = var.stage_bucket_retention_days
  terraform_state_bucket    = var.terraform_state_bucket
}

# BigQuery Module
module "bigquery" {
  source = "./modules/bigquery"

  project_id              = var.project_id
  region                  = var.region
  dataset_id              = var.dataset_id
  delete_contents_on_destroy = var.delete_contents_on_destroy
}

# Composer Module
module "composer" {
  source           = "./modules/composer"
  project_id       = var.project_id
  region           = var.region
  environment_name = "aws-billing-environment"
  service_account  = var.service_account
  drive_sa_email   = var.drive_sa_email
  dbt_sa_email     = var.dbt_sa_email
  raw_bucket       = var.raw_bucket
  stage_bucket     = var.stage_bucket
  dataset_id       = var.dataset_id
  image_version    = var.image_version
} 