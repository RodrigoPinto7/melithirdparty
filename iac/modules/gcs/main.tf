# Bucket para el estado de Terraform
# resource "google_storage_bucket" "terraform_state" {
#   name          = var.terraform_state_bucket
#   location      = var.region
#   force_destroy = false
#   uniform_bucket_level_access = true
#   storage_class = "STANDARD"
#
#   versioning {
#     enabled = true
#   }
# }

# Crear bucket de Raw
# resource "google_storage_bucket" "raw" {
#   name          = var.raw_bucket
#   location      = var.region
#   force_destroy = false
#   uniform_bucket_level_access = true
#   storage_class = "STANDARD"
#
#   versioning {
#     enabled = true
#   }
#
#   lifecycle_rule {
#     condition {
#       age = var.raw_bucket_retention_days
#     }
#     action {
#       type = "Delete"
#     }
#   }
# }

# Crear bucket de Stage
# resource "google_storage_bucket" "stage" {
#   name          = var.stage_bucket
#   location      = var.region
#   force_destroy = false
#   uniform_bucket_level_access = true
#   storage_class = "STANDARD"
#
#   versioning {
#     enabled = true
#   }
#
#   lifecycle_rule {
#     condition {
#       age = var.stage_bucket_retention_days
#     }
#     action {
#       type = "Delete"
#     }
#   }
# } 