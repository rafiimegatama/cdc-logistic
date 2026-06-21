"""
Chain 4: Failed delivery retry handler
Listens to delivery-failed-sub → retries with exponential backoff
Max 3 attempts → DLQ after exhaustion
"""
import json
import uuid
import time
import sys
import os
from datetime import datetime, timezone
from google.cloud import pubsub_v1, bigquery

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

log = config.setup_logging("retry_handler")

publisher  = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
bq_client  = bigquery.Client(project=config.GCP_PROJECT_ID)

# Track retry counts per shipment in memory
retry_counts = {}

metrics = {
    "received":  0,
    "retried":   0,
    "exhausted": 0,
    "errors":    0
}

# ==========================================
# LOG RETRY ATTEMPT TO BIGQUERY
# ==========================================
def log_retry(
    shipment_id:     int,
    order_id:        int,
    attempt:         int,
    retry_after_sec: int,
    status:          str,
    error_reason:    str = None
):
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "retry_id":        str(uuid.uuid4()),
            "shipment_id":     shipment_id,
            "order_id":        order_id,
            "attempt":         attempt,
            "max_attempts":    config.MAX_RETRY_ATTEMPTS,
            "event_type":      "FAILED",
            "error_reason":    error_reason or "delivery_failed",
            "retry_after_sec": retry_after_sec,
            "status":          status,
            "created_at":      now
        }
        table_ref = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.retry_log"
        errs      = bq_client.insert_rows_json(table_ref, [row])
        if errs:
            log.error(f"❌ retry_log BQ error: {errs}")
        else:
            log.info(
                f"📝 Retry logged | shipment={shipment_id} | "
                f"attempt={attempt}/{config.MAX_RETRY_ATTEMPTS} | status={status}"
            )
    except Exception as e:
        log.error(f"❌ Failed to log retry: {e}")

# ==========================================
# SEND TO DLQ
# ==========================================
def send_to_dlq(data: dict, reason: str):
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "table_id":      "delivery_events",
            "operation":     "FAILED_RETRY_EXHAUSTED",
            "error_reason":  reason,
            "raw_data":      json.dumps(data)[:10000],
            "cdc_timestamp": now,
            "created_at":    now
        }
        table_ref = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.dead_letter_queue"
        errs      = bq_client.insert_rows_json(table_ref, [row])
        if not errs:
            metrics["exhausted"] += 1
            log.warning(
                f"☠️  DLQ | shipment={data.get('shipment_id')} | "
                f"reason={reason} | max_attempts={config.MAX_RETRY_ATTEMPTS}"
            )
    except Exception as e:
        log.error(f"❌ Failed to send to DLQ: {e}")

# ==========================================
# RE-PUBLISH RETRY EVENT
# ==========================================
def republish_retry(data: dict, ordering_key: str, attempt: int):
    """Re-publish the event back to cdc-delivery for retry."""
    retry_payload = {
        **data,
        "retry_attempt":  attempt,
        "cdc_operation":  "INSERT",
        "cdc_timestamp":  datetime.now(timezone.utc).isoformat(),
        "is_retry":       True
    }

    topic_path = publisher.topic_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_TOPICS["delivery_events"]
    )

    attributes = {
        "kafka_topic":   "retry_handler",
        "cdc_operation": "INSERT",
        "event_type":    data.get("event_type", "FAILED"),
        "is_retry":      "true",
        "attempt":       str(attempt),
    }

    try:
        future = publisher.publish(
            topic_path,
            json.dumps(retry_payload).encode("utf-8"),
            ordering_key=ordering_key,
            **attributes
        )
        msg_id = future.result(timeout=config.PUBLISH_TIMEOUT)
        metrics["retried"] += 1
        log.info(
            f"🔄 Retry published | shipment={data.get('shipment_id')} | "
            f"attempt={attempt} | msg_id={msg_id}"
        )
        return True
    except Exception as e:
        log.error(f"❌ Failed to republish retry: {e}")
        metrics["errors"] += 1
        return False

# ==========================================
# CALLBACK: delivery-failed-sub
# ==========================================
def handle_failed_delivery(message: pubsub_v1.subscriber.message.Message):
    try:
        metrics["received"] += 1

        # Skip messages that are already retries at max attempt
        is_retry = message.attributes.get("is_retry", "false") == "true"
        attempt  = int(message.attributes.get("attempt", "0"))

        data        = json.loads(message.data.decode("utf-8"))
        payload     = data.get("data") or data.get("after") or data
        shipment_id = payload.get("shipment_id")
        order_id    = payload.get("order_id") or payload.get("shipment_id")
        event_type  = payload.get("event_type", "FAILED")

        if not shipment_id:
            message.ack()
            return

        # Track retry count
        if shipment_id not in retry_counts:
            retry_counts[shipment_id] = 0
        retry_counts[shipment_id] += 1
        current_attempt = retry_counts[shipment_id]

        ordering_key = config.build_ordering_key(
            {"order_id": order_id or shipment_id},
            "FAILED"
        )

        log.warning(
            f"⚠️  Failed delivery | shipment={shipment_id} | "
            f"attempt={current_attempt}/{config.MAX_RETRY_ATTEMPTS}"
        )

        if current_attempt >= config.MAX_RETRY_ATTEMPTS:
            # Exhausted — send to DLQ
            send_to_dlq(
                payload,
                f"Max retries ({config.MAX_RETRY_ATTEMPTS}) exhausted"
            )
            log_retry(
                shipment_id     = shipment_id,
                order_id        = order_id or shipment_id,
                attempt         = current_attempt,
                retry_after_sec = 0,
                status          = "EXHAUSTED"
            )
            # Reset counter
            del retry_counts[shipment_id]
            message.ack()
            return

        # Exponential backoff: 2^attempt seconds
        backoff = config.RETRY_BACKOFF_BASE ** current_attempt
        log.info(
            f"⏳ Retry backoff | shipment={shipment_id} | "
            f"waiting {backoff}s before attempt {current_attempt + 1}"
        )

        log_retry(
            shipment_id     = shipment_id,
            order_id        = order_id or shipment_id,
            attempt         = current_attempt,
            retry_after_sec = backoff,
            status          = "RETRYING"
        )

        # Wait then republish
        time.sleep(backoff)
        republish_retry(payload, ordering_key, current_attempt + 1)

        message.ack()

        if metrics["received"] % 5 == 0:
            log.info(
                f"📊 Retry Handler | "
                f"received={metrics['received']} "
                f"retried={metrics['retried']} "
                f"exhausted={metrics['exhausted']} "
                f"active_shipments={len(retry_counts)}"
            )

    except Exception as e:
        log.error(f"❌ Retry handler callback error: {e}")
        message.nack()

# ==========================================
# MAIN
# ==========================================
def main():
    log.info("🚀 Starting Failed Delivery Retry Handler")
    log.info(f"   Listening:    delivery-failed-sub")
    log.info(f"   Max retries:  {config.MAX_RETRY_ATTEMPTS}")
    log.info(f"   Backoff base: {config.RETRY_BACKOFF_BASE}^n seconds")

    sub_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        "delivery-failed-sub"
    )
    future = subscriber.subscribe(sub_path, callback=handle_failed_delivery)
    log.info(f"✅ Subscribed to {sub_path}")
    log.info("⏳ Waiting for failed deliveries...")

    try:
        future.result()
    except KeyboardInterrupt:
        log.info("🛑 Shutting down retry handler...")
        future.cancel()

if __name__ == "__main__":
    main()
