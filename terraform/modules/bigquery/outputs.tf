output "dataset_id" {
  value = google_bigquery_dataset.dataset.dataset_id
}

output "table_ids" {
  value = {
    for k, v in google_bigquery_table.tables : k => v.id
  }
}
