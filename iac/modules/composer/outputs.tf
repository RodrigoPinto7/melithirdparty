output "environment_name" {
  description = "The name of the Composer environment"
  value       = google_composer_environment.composer_env_update.name
}

output "airflow_uri" {
  description = "The URI of the Airflow web interface"
  value       = google_composer_environment.composer_env_update.config[0].airflow_uri
} 