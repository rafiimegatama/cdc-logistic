resource "google_pubsub_topic" "topics" {
  for_each = var.topics
  name     = each.key
  project  = var.project_id

  message_retention_duration = "${each.value.retention_days * 86400}s"
}

resource "google_pubsub_subscription" "subscriptions" {
  for_each = var.subscriptions
  name     = each.key
  topic    = google_pubsub_topic.topics[each.value.topic].name
  project  = var.project_id

  ack_deadline_seconds    = each.value.ack_deadline_seconds
  enable_message_ordering = each.value.enable_message_ordering
  retain_acked_messages   = each.value.retain_acked_messages
  filter                  = each.value.filter != "" ? each.value.filter : null

  message_retention_duration = "${each.value.retention_days * 86400}s"

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}
