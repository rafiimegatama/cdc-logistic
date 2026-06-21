variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "asia-southeast2"
}

variable "tables" {
  description = "Map of table name to schema file path"
  type = map(object({
    schema      = string
    description = optional(string, "")
    partition   = optional(bool, false)
    partition_field = optional(string, "")
  }))
}
