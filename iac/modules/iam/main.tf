# Service account for Drive to GCS script
resource "google_service_account" "drive_to_gcs_sa" {
  account_id   = "drive-to-gcs-sa"
  display_name = "Service Account for Drive to GCS Script"
}

# Service account for DBT
resource "google_service_account" "dbt_sa" {
  account_id   = "dbt-sa"
  display_name = "Service Account for DBT"
}

# Service account for Composer
resource "google_service_account" "composer_service_account" {
  account_id   = "composer-service-account"
  display_name = "Service Account for Cloud Composer"
}

# IAM roles for Drive to GCS service account
resource "google_project_iam_member" "drive_to_gcs_sa_roles" {
  for_each = toset([
    "roles/storage.objectViewer",  # Read from Drive
    "roles/storage.objectCreator", # Write to GCS
    "roles/bigquery.dataViewer",   # Read from BigQuery if needed
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.drive_to_gcs_sa.email}"
}

# IAM roles for DBT service account
resource "google_project_iam_member" "dbt_sa_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectViewer",
    "roles/storage.objectCreator",
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dbt_sa.email}"
}

# IAM roles for Composer service account
resource "google_project_iam_member" "composer_service_account_roles" {
  for_each = toset([
    "roles/composer.worker",
    "roles/storage.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/iam.serviceAccountUser"
  ])
  
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.composer_service_account.email}"
}

# Create service account keys (optional, only if needed)
resource "google_service_account_key" "drive_to_gcs_key" {
  service_account_id = google_service_account.drive_to_gcs_sa.name
}

resource "google_service_account_key" "dbt_key" {
  service_account_id = google_service_account.dbt_sa.name
} 