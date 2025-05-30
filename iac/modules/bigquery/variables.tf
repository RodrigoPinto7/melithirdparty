variable "project_id" {
  description = "The project ID where the dataset will be created"
  type        = string
}

variable "region" {
  description = "The region where the dataset will be created"
  type        = string
}

variable "dataset_id" {
  description = "The ID of the BigQuery dataset"
  type        = string
}

variable "delete_contents_on_destroy" {
  description = "If set to true, delete all the tables in the dataset when destroying the resource"
  type        = bool
  default     = true
} 