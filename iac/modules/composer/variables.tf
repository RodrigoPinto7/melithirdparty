variable "project_id" {
  description = "The project ID where the Composer environment will be created"
  type        = string
}

variable "region" {
  description = "The region where the Composer environment will be created"
  type        = string
}

variable "environment_name" {
  description = "The name of the Composer environment"
  type        = string
}

variable "image_version" {
  description = "The version of the Composer image"
  type        = string
  default     = "composer-2.13.1-airflow-2.10.5"
}

variable "env_variables" {
  description = "Additional environment variables"
  type        = map(string)
  default     = {}
}

variable "machine_type" {
  description = "The machine type to use"
  type        = string
  default     = "n1-standard-2"
}

variable "disk_size_gb" {
  description = "The disk size in GB"
  type        = number
  default     = 100
}

variable "network" {
  description = "The network to use"
  type        = string
  default     = "default"
}

variable "subnetwork" {
  description = "The subnetwork to use"
  type        = string
  default     = "default"
}

variable "service_account" {
  description = "The service account to use"
  type        = string
}

variable "dataset_id" {
  description = "The ID of the BigQuery dataset"
  type        = string
}

variable "raw_bucket" {
  description = "The name of the raw data bucket"
  type        = string
}

variable "stage_bucket" {
  description = "The name of the staging data bucket"
  type        = string
}

variable "drive_sa_email" {
  description = "The email of the Drive service account"
  type        = string
}

variable "dbt_sa_email" {
  description = "The email of the DBT service account"
  type        = string
}

variable "maintenance_window_start" {
  description = "The start time of the maintenance window"
  type        = string
  default     = "2024-01-01T00:00:00Z"
}

variable "maintenance_window_end" {
  description = "The end time of the maintenance window"
  type        = string
  default     = "2024-01-01T12:00:00Z"
}

variable "maintenance_window_recurrence" {
  description = "The recurrence of the maintenance window"
  type        = string
  default     = "FREQ=WEEKLY;BYDAY=SU"
} 