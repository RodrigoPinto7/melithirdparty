output "composer_service_account" {
  description = "The email of the Composer service account"
  value       = google_service_account.composer_service_account.email
}

output "drive_to_gcs_service_account" {
  description = "The email of the Drive to GCS service account"
  value       = google_service_account.drive_to_gcs_sa.email
}

output "dbt_service_account" {
  description = "The email of the DBT service account"
  value       = google_service_account.dbt_sa.email
}

output "drive_to_gcs_key" {
  description = "The private key for the Drive to GCS service account"
  value       = base64decode(google_service_account_key.drive_to_gcs_key.private_key)
  sensitive   = true
}

output "dbt_key" {
  description = "The private key for the DBT service account"
  value       = base64decode(google_service_account_key.dbt_key.private_key)
  sensitive   = true
} 