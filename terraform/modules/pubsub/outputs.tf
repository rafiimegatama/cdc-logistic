output "topic_ids" {
  value = {
    for k, v in google_pubsub_topic.topics : k => v.id
  }
}

output "subscription_ids" {
  value = {
    for k, v in google_pubsub_subscription.subscriptions : k => v.id
  }
}
