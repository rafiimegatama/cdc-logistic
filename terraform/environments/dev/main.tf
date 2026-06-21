terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ==========================================
# PUB/SUB MODULE
# ==========================================
module "pubsub" {
  source     = "../../modules/pubsub"
  project_id = var.project_id

  topics = {
    "cdc-orders" = {
      retention_days = 7
    }
    "cdc-order-items" = {
      retention_days = 7
    }
    "cdc-shipments" = {
      retention_days = 7
    }
    "cdc-delivery" = {
      retention_days = 7
    }
  }

  subscriptions = {
    # ── Existing base subscriptions ──
    "cdc-orders-sub" = {
      topic                   = "cdc-orders"
      enable_message_ordering = true
      retention_days          = 7
    }
    "cdc-order-items-sub" = {
      topic                   = "cdc-order-items"
      enable_message_ordering = true
      retention_days          = 7
    }
    "cdc-shipments-sub" = {
      topic                   = "cdc-shipments"
      enable_message_ordering = true
      retention_days          = 7
    }
    "cdc-delivery-sub" = {
      topic                   = "cdc-delivery"
      enable_message_ordering = true
      retention_days          = 7
    }

    # ── Chain 1: Order lifecycle (UPDATE only) ──
    "orders-lifecycle-sub" = {
      topic                   = "cdc-orders"
      filter                  = "attributes.cdc_operation = \"UPDATE\""
      enable_message_ordering = true
      retention_days          = 7
    }

    # ── Chain 2: Shipment per courier ──
    "shipments-jne-sub" = {
      topic                   = "cdc-shipments"
      filter                  = "attributes.courier = \"JNE\""
      enable_message_ordering = true
      retention_days          = 7
    }
    "shipments-sicepat-sub" = {
      topic                   = "cdc-shipments"
      filter                  = "attributes.courier = \"SiCepat\""
      enable_message_ordering = true
      retention_days          = 7
    }
    "shipments-all-sub" = {
      topic                   = "cdc-shipments"
      enable_message_ordering = true
      retention_days          = 7
    }

    # ── Chain 3: Cross-topic order SHIPPED trigger ──
    "orders-shipped-sub" = {
      topic                   = "cdc-orders"
      filter                  = "attributes.order_status = \"SHIPPED\""
      enable_message_ordering = true
      retention_days          = 7
    }

    # ── Chain 4: Failed delivery retry ──
    "delivery-failed-sub" = {
      topic                   = "cdc-delivery"
      filter                  = "attributes.event_type = \"FAILED\""
      enable_message_ordering = true
      retention_days          = 7
    }
  }
}

# ==========================================
# BIGQUERY MODULE
# ==========================================
module "bigquery" {
  source     = "../../modules/bigquery"
  project_id = var.project_id
  dataset_id = var.bq_dataset
  location   = var.region

  tables = {
    # ── Raw CDC tables ──
    "orders" = {
      schema      = file("${path.module}/../../schemas/orders.json")
      description = "Raw CDC events for orders"
    }
    "order_items" = {
      schema      = file("${path.module}/../../schemas/order_items.json")
      description = "Raw CDC events for order items"
    }
    "shipments" = {
      schema      = file("${path.module}/../../schemas/shipments.json")
      description = "Raw CDC events for shipments"
    }
    "delivery_events" = {
      schema      = file("${path.module}/../../schemas/delivery_events.json")
      description = "Raw CDC events for delivery"
    }
    "dead_letter_queue" = {
      schema      = file("${path.module}/../../schemas/dead_letter_queue.json")
      description = "Failed records DLQ"
    }

    # ── Chain event tables (NEW) ──
    "chain_events" = {
      schema      = file("${path.module}/../../schemas/chain_events.json")
      description = "Cross-topic chain reaction events"
      partition   = true
      partition_field = "event_timestamp"
    }
    "retry_log" = {
      schema      = file("${path.module}/../../schemas/retry_log.json")
      description = "Failed delivery retry attempts log"
      partition   = true
      partition_field = "created_at"
    }

    # ── Mart tables ──
    "mart_orders" = {
      schema      = file("${path.module}/../../schemas/mart_orders.json")
      description = "Deduplicated order mart"
    }
    "mart_shipments" = {
      schema      = file("${path.module}/../../schemas/mart_shipments.json")
      description = "Deduplicated shipment mart"
    }
    "mart_order_summary" = {
      schema      = file("${path.module}/../../schemas/mart_order_summary.json")
      description = "Order items summary mart"
    }
    "flink_order_agg" = {
      schema      = file("${path.module}/../../schemas/flink_order_agg.json")
      description = "Flink 5-min tumbling window order aggregation"
      partition   = true
      partition_field = "window_start"
    }
    "flink_delivery_sla" = {
      schema      = file("${path.module}/../../schemas/flink_delivery_sla.json")
      description = "Flink delivery SLA breach detection"
      partition   = true
      partition_field = "window_start"
    }
    "flink_anomalies" = {
      schema      = file("${path.module}/../../schemas/flink_anomalies.json")
      description = "Flink anomaly detection output"
      partition   = true
      partition_field = "detected_at"
    }
    "mart_delivery_kpi" = {
      schema      = file("${path.module}/../../schemas/mart_delivery_kpi.json")
      description = "Delivery KPI mart"
    }
  }
}
