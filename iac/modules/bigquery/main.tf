# Crear dataset de BigQuery
resource "google_bigquery_dataset" "billing_dataset" {
  dataset_id                  = var.dataset_id
  location                   = var.region
  delete_contents_on_destroy = var.delete_contents_on_destroy

  lifecycle {
    prevent_destroy = true
  }
} 