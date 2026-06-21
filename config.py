"""
Central configuration for CDC Logistics Pipeline
All settings loaded from .env — never hardcoded
"""
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# GCP
# ==========================================
GCP_PROJECT_ID  = os.getenv("GCP_PROJECT_ID")
BQ_DATASET      = os.getenv("BIGQUERY_DATASET", "logistics_dwh")
BQ_LOCATION     = os.getenv("BQ_LOCATION", "asia-southeast2")

# ==========================================
# PUB/SUB TOPICS
# ==========================================
PUBSUB_TOPICS = {
    "orders":          os.getenv("PUBSUB_TOPIC_ORDERS",      "cdc-orders"),
    "order_items":     os.getenv("PUBSUB_TOPIC_ORDER_ITEMS", "cdc-order-items"),
    "shipments":       os.getenv("PUBSUB_TOPIC_SHIPMENTS",   "cdc-shipments"),
    "delivery_events": os.getenv("PUBSUB_TOPIC_DELIVERY",    "cdc-delivery"),
}

# ==========================================
# PUB/SUB SUBSCRIPTIONS
# ==========================================
PUBSUB_SUBSCRIPTIONS = {
    "cdc-orders-sub":      "orders",
    "cdc-order-items-sub": "order_items",
    "cdc-shipments-sub":   "shipments",
    "cdc-delivery-sub":    "delivery_events",
}

# ==========================================
# KAFKA
# ==========================================
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPICS     = [
    "cdc.logistics.orders",
    "cdc.logistics.order_items",
    "cdc.logistics.shipments",
    "cdc.logistics.delivery_events",
]
KAFKA_GROUP_ID   = os.getenv("KAFKA_GROUP_ID", "cdc-pubsub-bridge-v2")

# Map Kafka topic → Pub/Sub topic name
KAFKA_TO_PUBSUB = {
    "cdc.logistics.orders":          PUBSUB_TOPICS["orders"],
    "cdc.logistics.order_items":     PUBSUB_TOPICS["order_items"],
    "cdc.logistics.shipments":       PUBSUB_TOPICS["shipments"],
    "cdc.logistics.delivery_events": PUBSUB_TOPICS["delivery_events"],
}

# Map Kafka topic → Schema Registry subject
KAFKA_TO_SCHEMA = {
    "cdc.logistics.orders":          "cdc-orders-value",
    "cdc.logistics.order_items":     "cdc-order-items-value",
    "cdc.logistics.shipments":       "cdc-shipments-value",
    "cdc.logistics.delivery_events": "cdc-delivery-value",
}

# ==========================================
# POSTGRESQL
# ==========================================
PG_HOST     = os.getenv("PG_HOST",     "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("POSTGRES_DB",       "logistics_db")
PG_USER     = os.getenv("POSTGRES_USER",     "cdcuser")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "cdcpassword")

# ==========================================
# SCHEMA REGISTRY
# ==========================================
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# ==========================================
# DEBEZIUM
# ==========================================
DEBEZIUM_URL       = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
DEBEZIUM_CONNECTOR = "logistics-cdc-connector"

# ==========================================
# PIPELINE SETTINGS
# ==========================================
MAX_RETRIES         = int(os.getenv("MAX_RETRIES",         "3"))
RETRY_BACKOFF_BASE  = int(os.getenv("RETRY_BACKOFF_BASE",  "2"))
PUBLISH_TIMEOUT     = int(os.getenv("PUBLISH_TIMEOUT",     "10"))
METRICS_LOG_EVERY   = int(os.getenv("METRICS_LOG_EVERY",   "20"))
GENERATOR_MIN_SLEEP = int(os.getenv("GENERATOR_MIN_SLEEP", "3"))
GENERATOR_MAX_SLEEP = int(os.getenv("GENERATOR_MAX_SLEEP", "8"))

# ==========================================
# BIGQUERY TIMESTAMP COLUMNS
# ==========================================
BQ_TIMESTAMP_COLS = {
    "orders":          ["order_date", "updated_at"],
    "order_items":     [],
    "shipments":       ["shipped_at", "estimated_arrival", "updated_at"],
    "delivery_events": ["event_time"],
}

# ==========================================
# BIGQUERY VALID COLUMNS PER TABLE
# ==========================================
BQ_VALID_COLS = {
    "orders": [
        "order_id", "customer_id", "order_status", "total_amount",
        "payment_method", "order_date", "updated_at",
        "cdc_operation", "cdc_timestamp"
    ],
    "order_items": [
        "item_id", "order_id", "product_id", "quantity",
        "unit_price", "subtotal", "cdc_operation", "cdc_timestamp"
    ],
    "shipments": [
        "shipment_id", "order_id", "courier", "tracking_number",
        "origin_city", "destination_city", "shipment_status",
        "shipped_at", "estimated_arrival", "updated_at",
        "cdc_operation", "cdc_timestamp"
    ],
    "delivery_events": [
        "event_id", "shipment_id", "event_type", "event_location",
        "event_note", "event_time", "cdc_operation", "cdc_timestamp"
    ],
}

# ==========================================
# MART TABLES
# ==========================================
MART_TABLES = [
    "mart_orders",
    "mart_shipments",
    "mart_order_summary",
    "mart_delivery_kpi",
]

STORED_PROCEDURES = [
    "sp_deduplicate_orders",
    "sp_deduplicate_shipments",
    "sp_order_summary",
    "sp_delivery_kpi",
]

# ==========================================
# LOGGING SETUP
# ==========================================
def setup_logging(name: str, level: str = "INFO") -> logging.Logger:
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    )
    return logging.getLogger(name)

# ==========================================
# VALIDATION
# ==========================================
def validate_config() -> list[str]:
    """Check all required config values are set. Returns list of missing keys."""
    required = {
        "GCP_PROJECT_ID":  GCP_PROJECT_ID,
        "BIGQUERY_DATASET": BQ_DATASET,
    }
    missing = [k for k, v in required.items() if not v]
    return missing

if __name__ == "__main__":
    log = setup_logging("config")
    missing = validate_config()
    if missing:
        log.error(f"❌ Missing config: {missing}")
    else:
        log.info("✅ All config values loaded")
        log.info(f"   GCP Project  : {GCP_PROJECT_ID}")
        log.info(f"   BQ Dataset   : {BQ_DATASET}")
        log.info(f"   Kafka topics : {KAFKA_TOPICS}")
        log.info(f"   Pub/Sub      : {PUBSUB_TOPICS}")
        log.info(f"   Schema Reg   : {SCHEMA_REGISTRY_URL}")
        log.info(f"   Debezium     : {DEBEZIUM_URL}")

# ==========================================
# CHAIN REACTION SUBSCRIPTIONS
# ==========================================
CHAIN_SUBSCRIPTIONS = {
    "orders-lifecycle-sub":  "orders",
    "orders-shipped-sub":    "orders",
    "shipments-jne-sub":     "shipments",
    "shipments-sicepat-sub": "shipments",
    "shipments-all-sub":     "shipments",
    "delivery-failed-sub":   "delivery_events",
}

# Ordering key builder
def build_ordering_key(data: dict, event_type: str) -> str:
    order_id = data.get("order_id", "unknown")
    return f"{order_id}:{event_type}"

# Max retry attempts for failed delivery
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
