variable "project_id" {
  description = "The project ID where resources will be created"
  type        = string
  default     = "melithirdparty"
}

variable "region" {
  description = "The region where resources will be created"
  type        = string
  default     = "us-central1"
}

variable "terraform_state_bucket" {
  description = "The name of the GCS bucket for Terraform state"
  type        = string
  default     = "terraform-state-melithirdparty-460619"
}

variable "raw_bucket" {
  description = "The name of the GCS bucket for raw data"
  type        = string
  default     = "raw-data-melithirdparty-460619"
}

variable "stage_bucket" {
  description = "The name of the GCS bucket for staged data"
  type        = string
  default     = "stage-data-melithirdparty-460619"
}

variable "raw_bucket_retention_days" {
  description = "Number of days to retain raw data"
  type        = number
  default     = 30
}

variable "stage_bucket_retention_days" {
  description = "Number of days to retain staged data"
  type        = number
  default     = 7
}

variable "dataset_id" {
  description = "The ID of the BigQuery dataset"
  type        = string
  default     = "third_party_data"
}

variable "delete_contents_on_destroy" {
  description = "If set to true, delete all the tables in the dataset when destroying the resource"
  type        = bool
  default     = false
}

variable "environment_name" {
  description = "The name of the Composer environment"
  type        = string
  default     = "aws-billing-environment"
}


variable "service_account" {
  description = "The service account email for the Composer environment"
  type        = string
}

variable "drive_sa_email" {
  description = "The service account email for Drive access"
  type        = string
}

variable "dbt_sa_email" {
  description = "The service account email for DBT"
  type        = string
}

variable "image_version" {
  description = "The version of the Composer image"
  type        = string
} 