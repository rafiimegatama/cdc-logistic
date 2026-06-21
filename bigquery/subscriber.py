import json
import logging
import os
import sys
from datetime import datetime, timezone
from google.cloud import pubsub_v1, bigquery
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_quality.expectations import validate, validation_report

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET")

SUBSCRIPTION_MAP = {
    "cdc-orders-sub":      "orders",
    "cdc-order-items-sub": "order_items",
    "cdc-shipments-sub":   "shipments",
    "cdc-delivery-sub":    "delivery_events"
}

TIMESTAMP_COLS = {
    "orders":          ["order_date", "updated_at"],
    "order_items":     [],
    "shipments":       ["shipped_at", "estimated_arrival", "updated_at"],
    "delivery_events": ["event_time"]
}

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

# ==========================================
# METRICS
# ==========================================
metrics = {
    "total_received":  0,
    "total_inserted":  0,
    "total_rejected":  0,
    "total_warnings":  0,
    "by_table":        {}
}

def log_metrics():
    log.info(
        f"📊 DQ Metrics | "
        f"received={metrics['total_received']} "
        f"inserted={metrics['total_inserted']} "
        f"rejected={metrics['total_rejected']} "
        f"warnings={metrics['total_warnings']} "
        f"by_table={metrics['by_table']}"
    )

# ==========================================
# TIMESTAMP CONVERSION
# ==========================================
def convert_timestamps(data: dict, table_id: str) -> dict:
    ts_cols = TIMESTAMP_COLS.get(table_id, [])
    for col in ts_cols:
        if col in data and data[col] is not None:
            try:
                micros    = int(data[col])
                seconds   = micros / 1_000_000
                dt        = datetime.fromtimestamp(seconds, tz=timezone.utc)
                data[col] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                log.warning(f"⚠️ Could not convert {col}: {data[col]} — {e}")
                data[col] = None
    return data

def filter_columns(data: dict, table_id: str) -> dict:
    valid = VALID_COLS.get(table_id, [])
    return {k: v for k, v in data.items() if k in valid}

# ==========================================
# DEAD LETTER QUEUE
# ==========================================
def send_to_dlq(
    table_id:  str,
    operation: str,
    data:      dict,
    errors:    list
):
    """Send failed records to dead_letter_queue table in BigQuery."""
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "table_id":     table_id,
            "operation":    operation,
            "error_reason": json.dumps(errors),
            "raw_data":     json.dumps(data)[:10000],
            "cdc_timestamp": now,
            "created_at":   now
        }
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.dead_letter_queue"
        errs      = bq_client.insert_rows_json(table_ref, [row])

        if errs:
            log.error(f"❌ DLQ insert error: {errs}")
        else:
            log.warning(
                f"☠️ DLQ | table={table_id} | "
                f"op={operation} | errors={errors}"
            )
            metrics["total_rejected"] += 1

    except Exception as e:
        log.error(f"❌ Failed to send to DLQ: {e}")

# ==========================================
# INSERT TO BIGQUERY
# ==========================================
def insert_to_bigquery(table_id: str, data: dict, operation: str):
    try:
        if data is None:
            return

        row = dict(data)
        row["cdc_operation"] = operation
        row["cdc_timestamp"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # ── Data Quality Validation ──
        is_valid, errors, warnings = validate(row, table_id)

        if warnings:
            metrics["total_warnings"] += len(warnings)
            for w in warnings:
                log.warning(f"⚠️ DQ Warning | {table_id} | {w}")

        if not is_valid:
            send_to_dlq(table_id, operation, row, errors)
            return

        # ── Convert timestamps ──
        row = convert_timestamps(row, table_id)

        # ── Filter to valid BQ columns ──
        row = filter_columns(row, table_id)

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        errs      = bq_client.insert_rows_json(table_ref, [row])

        if errs:
            log.error(f"❌ BQ insert error for {table_id}: {errs}")
            send_to_dlq(table_id, operation, row, [str(errs)])
        else:
            metrics["total_inserted"] += 1
            metrics["by_table"][table_id] = \
                metrics["by_table"].get(table_id, 0) + 1
            log.info(f"✅ Inserted to BQ {table_id} | op={operation}")

    except Exception as e:
        log.error(f"❌ Failed to insert to BigQuery: {e}")
        send_to_dlq(table_id, operation, data or {}, [str(e)])

# ==========================================
# CALLBACK
# ==========================================
def make_callback(table_id: str):
    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            metrics["total_received"] += 1
            data      = json.loads(message.data.decode("utf-8"))
            operation = message.attributes.get("cdc_operation", "UNKNOWN")
            payload   = data.get("data") or data.get("after")

            log.info(f"📨 Received | table={table_id} | op={operation}")
            insert_to_bigquery(table_id, payload, operation)
            message.ack()

            if metrics["total_received"] % 20 == 0:
                log_metrics()

        except Exception as e:
            log.error(f"❌ Callback error: {e}")
            message.nack()

    return callback

# ==========================================
# MAIN
# ==========================================
def main():
    log.info(f"🚀 Starting Pub/Sub → BigQuery subscriber with Data Quality")
    log.info(f"   Project : {PROJECT_ID}")
    log.info(f"   Dataset : {DATASET_ID}")

    futures = []
    for sub_id, table_id in SUBSCRIPTION_MAP.items():
        sub_path = subscriber.subscription_path(PROJECT_ID, sub_id)
        future   = subscriber.subscribe(
            sub_path, callback=make_callback(table_id)
        )
        futures.append(future)
        log.info(f"✅ Subscribed {sub_path} → BQ {table_id}")

    log.info("⏳ Listening for messages with data quality checks...")

    try:
        for future in futures:
            future.result()
    except KeyboardInterrupt:
        log.info("🛑 Shutting down subscriber...")
        for future in futures:
            future.cancel()

if __name__ == "__main__":
    main()
