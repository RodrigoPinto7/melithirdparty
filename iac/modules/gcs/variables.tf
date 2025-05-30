variable "project_id" {
  description = "The project ID where the buckets will be created"
  type        = string
}

variable "region" {
  description = "The region where the buckets will be created"
  type        = string
}

variable "terraform_state_bucket" {
  description = "The name of the bucket for Terraform state"
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

variable "raw_bucket_retention_days" {
  description = "Number of days to retain raw data"
  type        = number
  default     = 90
}

variable "stage_bucket_retention_days" {
  description = "Number of days to retain staging data"
  type        = number
  default     = 30
} 