variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "topics" {
  description = "Map of topic name to config"
  type = map(object({
    retention_days = number
  }))
}

variable "subscriptions" {
  description = "Map of subscription name to config"
  type = map(object({
    topic                  = string
    filter                 = optional(string, "")
    enable_message_ordering = optional(bool, false)
    ack_deadline_seconds   = optional(number, 60)
    retain_acked_messages  = optional(bool, false)
    retention_days         = optional(number, 7)
  }))
}
