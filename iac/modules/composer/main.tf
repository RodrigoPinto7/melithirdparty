data "google_composer_environment" "composer_env" {
  name    = var.environment_name
  project = var.project_id
  region  = var.region
}

# Update environment variables
resource "google_composer_environment" "composer_env_update" {
  name    = var.environment_name
  project = var.project_id
  region  = var.region

  config {
    software_config {
      env_variables = {
        "dataset_id"   = var.dataset_id
        "project_id"   = var.project_id
        "raw_bucket"   = var.raw_bucket
        "stage_bucket" = var.stage_bucket
      }
      # Preserve existing PyPI packages
      pypi_packages = data.google_composer_environment.composer_env.config[0].software_config[0].pypi_packages
    }
  }

  timeouts {
    update = "30m"
  }
}

# Get the DAGs bucket from the Composer environment
data "google_storage_bucket" "dags_bucket" {
  name = split("/", replace(data.google_composer_environment.composer_env.config[0].dag_gcs_prefix, "gs://", ""))[0]
}

# Upload the main DAG
resource "google_storage_bucket_object" "main_dag" {
  name   = "dags/third_party_data_pipeline.py"
  source = "${path.module}/../../../dags/third_party_data_pipeline.py"
  bucket = data.google_storage_bucket.dags_bucket.name
}

# Upload the scripts
resource "google_storage_bucket_object" "scripts" {
  for_each = fileset("${path.module}/../../../dags/scripts", "**/*")
  name   = "dags/scripts/${each.value}"
  source = "${path.module}/../../../dags/scripts/${each.value}"
  bucket = data.google_storage_bucket.dags_bucket.name
}


