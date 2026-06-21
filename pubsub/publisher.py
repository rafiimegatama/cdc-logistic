import json
import logging
import os
from datetime import datetime, timezone
from google.cloud import pubsub_v1, bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET")

SUBSCRIPTION_MAP = {
    "cdc-orders-sub":      "orders",
    "cdc-order-items-sub": "order_items",
    "cdc-shipments-sub":   "shipments",
    "cdc-delivery-sub":    "delivery_events"
}

# Timestamp columns per table — Debezium sends microseconds
TIMESTAMP_COLS = {
    "orders":          ["order_date", "updated_at"],
    "order_items":     [],
    "shipments":       ["shipped_at", "estimated_arrival", "updated_at"],
    "delivery_events": ["event_time"]
}

# Valid columns per BQ table
VALID_COLS = {
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
    ]
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

bq_client  = bigquery.Client(project=PROJECT_ID)
subscriber = pubsub_v1.SubscriberClient()

def convert_timestamps(data: dict, table_id: str) -> dict:
    """Convert Debezium microsecond timestamps to BQ-compatible ISO strings."""
    ts_cols = TIMESTAMP_COLS.get(table_id, [])
    for col in ts_cols:
        if col in data and data[col] is not None:
            try:
                # Debezium sends microseconds since epoch
                micros = int(data[col])
                seconds = micros / 1_000_000
                dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
                data[col] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                log.warning(f"⚠️ Could not convert {col}: {data[col]} — {e}")
                data[col] = None
    return data

def filter_columns(data: dict, table_id: str) -> dict:
    """Keep only columns that exist in the BQ table."""
    valid = VALID_COLS.get(table_id, [])
    return {k: v for k, v in data.items() if k in valid}

def insert_to_bigquery(table_id: str, data: dict, operation: str):
    """Insert CDC event data into BigQuery."""
    try:
        if data is None:
            return

        row = dict(data)
        row["cdc_operation"] = operation
        row["cdc_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Convert timestamps from microseconds
        row = convert_timestamps(row, table_id)

        # Filter to valid columns only
        row = filter_columns(row, table_id)

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        errors    = bq_client.insert_rows_json(table_ref, [row])

        if errors:
            log.error(f"❌ BQ insert errors for {table_id}: {errors}")
        else:
            log.info(f"✅ Inserted to BQ {table_id} | op={operation}")

    except Exception as e:
        log.error(f"❌ Failed to insert to BigQuery: {e}")

def make_callback(table_id: str):
    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            data      = json.loads(message.data.decode("utf-8"))
            operation = message.attributes.get("cdc_operation", "UNKNOWN")
            payload   = data.get("data") or data.get("after")

            log.info(f"📨 Received | table={table_id} | op={operation}")
            insert_to_bigquery(table_id, payload, operation)
            message.ack()

        except Exception as e:
            log.error(f"❌ Callback error: {e}")
            message.nack()

    return callback

def main():
    log.info(f"🚀 Starting Pub/Sub → BigQuery subscriber")
    log.info(f"   Project : {PROJECT_ID}")
    log.info(f"   Dataset : {DATASET_ID}")

    futures = []
    for sub_id, table_id in SUBSCRIPTION_MAP.items():
        sub_path = subscriber.subscription_path(PROJECT_ID, sub_id)
        future   = subscriber.subscribe(sub_path, callback=make_callback(table_id))
        futures.append(future)
        log.info(f"✅ Subscribed to {sub_path} → BQ table {table_id}")

    log.info("⏳ Listening for messages...")

    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        log.info("🛑 Shutting down subscriber...")
        for future in futures:
            future.cancel()

if __name__ == "__main__":
    main()
