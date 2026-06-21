variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-southeast2"
}

variable "bq_dataset" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "logistics_dwh"
}
