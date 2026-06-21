"""
Chain 3: Cross-topic reactor
Listens to orders-shipped-sub → auto-publishes to cdc-shipments
when order status = SHIPPED, triggering shipment creation chain
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

log = config.setup_logging("chain_reactor")

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher         = pubsub_v1.PublisherClient(publisher_options=publisher_options)
subscriber        = pubsub_v1.SubscriberClient()
bq_client         = bigquery.Client(project=config.GCP_PROJECT_ID)

COURIERS = ["JNE", "SiCepat", "JNT", "Anteraja", "TIKI"]

metrics = {
    "received":  0,
    "triggered": 0,
    "skipped":   0,
    "errors":    0
}

def log_chain_event(
    chain_name:   str,
    order_id:     int,
    from_status:  str,
    to_status:    str,
    ordering_key: str,
    courier:      str = None,
    triggered_by: str = "chain_reactor"
):
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "event_id":        str(uuid.uuid4()),
            "chain_name":      chain_name,
            "order_id":        order_id,
            "from_status":     from_status,
            "to_status":       to_status,
            "ordering_key":    ordering_key,
            "courier":         courier,
            "triggered_by":    triggered_by,
            "event_timestamp": now,
            "cdc_timestamp":   now
        }
        table_ref = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.chain_events"
        errs      = bq_client.insert_rows_json(table_ref, [row])
        if errs:
            log.error(f"❌ chain_events BQ error: {errs}")
        else:
            log.info(
                f"📝 Chain event | {chain_name} | "
                f"order={order_id} | {from_status}→{to_status}"
            )
    except Exception as e:
        log.error(f"❌ Failed to log chain event: {e}")

def publish_shipment_trigger(order_id: int, ordering_key: str):
    import random
    courier     = random.choice(COURIERS)
    tracking_no = f"{courier[:3].upper()}{random.randint(10000000, 99999999)}"

    payload = {
        "cdc_operation":  "INSERT",
        "cdc_timestamp":  datetime.now(timezone.utc).isoformat(),
        "schema_version": "1",
        "triggered_by":   "chain_reactor",
        "data": {
            "order_id":        order_id,
            "courier":         courier,
            "tracking_number": tracking_no,
            "shipment_status": "WAITING_PICKUP",
            "cdc_operation":   "INSERT",
            "cdc_timestamp":   datetime.now(timezone.utc).isoformat()
        }
    }

    topic_path = publisher.topic_path(
        config.GCP_PROJECT_ID,
        config.PUBSUB_TOPICS["shipments"]
    )

    # ── ordering_key is a positional kwarg to publish(), NOT in attributes ──
    attributes = {
        "kafka_topic":   "chain_reactor",
        "cdc_operation": "INSERT",
        "order_status":  "SHIPPED",
        "courier":       courier,
        "event_type":    "SHIPMENT_CREATED"
    }

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            future = publisher.publish(
                topic_path,
                json.dumps(payload).encode("utf-8"),
                ordering_key=ordering_key,
                **attributes
            )
            msg_id = future.result(timeout=config.PUBLISH_TIMEOUT)
            log.info(
                f"🔗 Chain triggered | order={order_id} | "
                f"courier={courier} | tracking={tracking_no} | "
                f"key={ordering_key} | msg_id={msg_id}"
            )
            metrics["triggered"] += 1
            return courier
        except Exception as e:
            if attempt < config.MAX_RETRIES:
                time.sleep(config.RETRY_BACKOFF_BASE ** attempt)
            else:
                log.error(f"❌ Failed to publish shipment trigger: {e}")
                metrics["errors"] += 1
                return None

def handle_shipped_order(message: pubsub_v1.subscriber.message.Message):
    try:
        metrics["received"] += 1
        data         = json.loads(message.data.decode("utf-8"))
        payload      = data.get("data") or data.get("after") or {}
        order_id     = payload.get("order_id")
        op           = message.attributes.get("cdc_operation", "")
        order_status = payload.get("order_status", "")

        if not order_id:
            metrics["skipped"] += 1
            message.ack()
            return

        if order_status != "SHIPPED":
            metrics["skipped"] += 1
            message.ack()
            return

        ordering_key = config.build_ordering_key(payload, "SHIPPED")
        log.info(f"⚡ Chain 3 triggered | order={order_id} | key={ordering_key}")

        courier = publish_shipment_trigger(order_id, ordering_key)

        if courier:
            log_chain_event(
                chain_name   = "order_to_shipment",
                order_id     = order_id,
                from_status  = "PROCESSING",
                to_status    = "SHIPPED",
                ordering_key = ordering_key,
                courier      = courier
            )

        message.ack()

        if metrics["received"] % 10 == 0:
            log.info(
                f"📊 Chain Reactor | "
                f"received={metrics['received']} "
                f"triggered={metrics['triggered']} "
                f"skipped={metrics['skipped']} "
                f"errors={metrics['errors']}"
            )

    except Exception as e:
        log.error(f"❌ Chain reactor callback error: {e}")
        message.nack()

def main():
    log.info("🚀 Starting Chain Reactor (Cross-topic handler)")
    log.info(f"   Listening: orders-shipped-sub")
    log.info(f"   Triggers:  cdc-shipments topic")
    log.info(f"   Ordering:  enabled")

    sub_path = subscriber.subscription_path(
        config.GCP_PROJECT_ID,
        "orders-shipped-sub"
    )
    future = subscriber.subscribe(sub_path, callback=handle_shipped_order)
    log.info(f"✅ Subscribed to {sub_path}")
    log.info("⏳ Waiting for SHIPPED orders...")

    try:
        future.result()
    except KeyboardInterrupt:
        log.info("🛑 Shutting down chain reactor...")
        future.cancel()

if __name__ == "__main__":
    main()
