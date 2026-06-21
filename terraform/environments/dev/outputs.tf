output "pubsub_topics" {
  value = module.pubsub.topic_ids
}

output "pubsub_subscriptions" {
  value = module.pubsub.subscription_ids
}

output "bigquery_dataset" {
  value = module.bigquery.dataset_id
}

output "bigquery_tables" {
  value = module.bigquery.table_ids
}
