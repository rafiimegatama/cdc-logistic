resource "google_bigquery_dataset" "dataset" {
  dataset_id  = var.dataset_id
  project     = var.project_id
  location    = var.location
  description = "CDC Logistics Pipeline Data Warehouse"

  delete_contents_on_destroy = false
}

resource "google_bigquery_table" "tables" {
  for_each   = var.tables
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = each.key
  project    = var.project_id
  description = each.value.description

  schema = each.value.schema

  dynamic "time_partitioning" {
    for_each = each.value.partition ? [1] : []
    content {
      type  = "DAY"
      field = each.value.partition_field
    }
  }

  deletion_protection = false
}
