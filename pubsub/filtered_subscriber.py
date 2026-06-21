"""
Chain 1: Order lifecycle tracker (UPDATE events only)
Chain 2: Shipment tracking per courier (JNE, SiCepat filtered)
Logs all state transitions to chain_events BQ table
"""
import json
import uuid
import sys
import os
from datetime import datetime, timezone
from google.cloud import pubsub_v1, bigquery

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

log = config.setup_logging("filtered_subscriber")

subscriber = pubsub_v1.SubscriberClient()
bq_client  = bigquery.Client(project=config.GCP_PROJECT_ID)

metrics = {
    "orders_lifecycle":   0,
    "shipments_jne":      0,
    "shipments_sicepat":  0,
    "chain_events_logged": 0,
    "errors":             0
}

# Order status transition map
ORDER_TRANSITIONS = {
    "PENDING":    "PROCESSING",
    "PROCESSING": "SHIPPED",
    "SHIPPED":    "DELIVERED",
    "DELIVERED":  None,
    "CANCELLED":  None
}

SHIPMENT_TRANSITIONS = {
    "WAITING_PICKUP":    "IN_TRANSIT",
    "IN_TRANSIT":        "OUT_FOR_DELIVERY",
    "OUT_FOR_DELIVERY":  "DELIVERED",
    "DELIVERED":         None,
    "FAILED":            "WAITING_PICKUP"  # retry path
}

# ==========================================
# LOG CHAIN EVENT
# ==========================================
def log_chain_event(
    chain_name:   str,
    order_id:     int,
    from_status:  str,
    to_status:    str,
    ordering_key: str,
    courier:      str = None,
    triggered_by: str = "filtered_subscriber"
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
        if not errs:
            metrics["chain_events_logged"] += 1
            log.info(
                f"📝 Chain event | {chain_name} | "
                f"order={order_id} | {from_status}→{to_status} | "
                f"key={ordering_key}"
            )
    except Exception as e:
        log.error(f"❌ Failed to log chain event: {e}")
        metrics["errors"] += 1

# ==========================================
# CHAIN 1: ORDER LIFECYCLE
# ==========================================
def handle_order_lifecycle(message: pubsub_v1.subscriber.message.Message):
    try:
        metrics["orders_lifecycle"] += 1
        data        = json.loads(message.data.decode("utf-8"))
        before      = data.get("before") or {}
        after       = data.get("data") or data.get("after") or {}
        order_id    = after.get("order_id")
        new_status  = after.get("order_status")
        prev_status = before.get("order_status") if before else None

        if not order_id or not new_status:
            message.ack()
            return

        ordering_key = config.build_ordering_key(after, new_status)
        expected_next = ORDER_TRANSITIONS.get(prev_status or "PENDING")

        # Detect valid transition
        if prev_status and new_status != prev_status:
            log_chain_event(
                chain_name   = "order_lifecycle",
                order_id     = order_id,
                from_status  = prev_status or "UNKNOWN",
                to_status    = new_status,
                ordering_key = ordering_key,
                triggered_by = "orders-lifecycle-sub"
            )

            # Warn on unexpected transitions
            if expected_next and new_status != expected_next:
                log.warning(
                    f"⚠️  Unexpected transition | order={order_id} | "
                    f"{prev_status}→{new_status} (expected {expected_next})"
                )
        message.ack()

    except Exception as e:
        log.error(f"❌ Order lifecycle callback error: {e}")
        metrics["errors"] += 1
        message.nack()

# ==========================================
# CHAIN 2: SHIPMENT TRACKING PER COURIER
# ==========================================
def make_shipment_callback(courier_name: str, metric_key: str):
    def callback(message: pubsub_v1.subscriber.message.Message):
        try:
            metrics[metric_key] += 1
            data        = json.loads(message.data.decode("utf-8"))
            before      = data.get("before") or {}
            after       = data.get("data") or data.get("after") or {}
            shipment_id = after.get("shipment_id")
            order_id    = after.get("order_id") or shipment_id
            new_status  = after.get("shipment_status")
            prev_status = before.get("shipment_status") if before else None
            courier     = after.get("courier", courier_name)

            if not shipment_id or not new_status:
                message.ack()
                return

            ordering_key = config.build_ordering_key(
                {"order_id": order_id}, new_status
            )

            if prev_status and new_status != prev_status:
                log_chain_event(
                    chain_name   = f"shipment_tracking_{courier.lower()}",
                    order_id     = order_id or shipment_id,
                    from_status  = prev_status or "UNKNOWN",
                    to_status    = new_status,
                    ordering_key = ordering_key,
                    courier      = courier,
                    triggered_by = f"shipments-{courier.lower()}-sub"
                )

                # Edge case: FAILED → log warning for retry chain
                if new_status == "FAILED":
                    log.warning(
                        f"🚨 Delivery FAILED | courier={courier} | "
                        f"shipment={shipment_id} | order={order_id} | "
                        f"→ retry_handler will pick this up"
                    )

            message.ack()

        except Exception as e:
            log.error(f"❌ Shipment {courier_name} callback error: {e}")
            metrics["errors"] += 1
            message.nack()

    return callback

# ==========================================
# MAIN
# ==========================================
def main():
    log.info("🚀 Starting Filtered Subscriber (Chain 1 + Chain 2)")
    log.info(f"   Chain 1: orders-lifecycle-sub (UPDATE only)")
    log.info(f"   Chain 2: shipments-jne-sub + shipments-sicepat-sub")

    futures = []

    # Chain 1 — Order lifecycle
    sub1 = subscriber.subscription_path(config.GCP_PROJECT_ID, "orders-lifecycle-sub")
    f1   = subscriber.subscribe(sub1, callback=handle_order_lifecycle)
    futures.append(f1)
    log.info(f"✅ Subscribed: orders-lifecycle-sub → Chain 1")

    # Chain 2 — JNE shipments
    sub2 = subscriber.subscription_path(config.GCP_PROJECT_ID, "shipments-jne-sub")
    f2   = subscriber.subscribe(
        sub2,
        callback=make_shipment_callback("JNE", "shipments_jne")
    )
    futures.append(f2)
    log.info(f"✅ Subscribed: shipments-jne-sub → Chain 2 JNE")

    # Chain 2 — SiCepat shipments
    sub3 = subscriber.subscription_path(config.GCP_PROJECT_ID, "shipments-sicepat-sub")
    f3   = subscriber.subscribe(
        sub3,
        callback=make_shipment_callback("SiCepat", "shipments_sicepat")
    )
    futures.append(f3)
    log.info(f"✅ Subscribed: shipments-sicepat-sub → Chain 2 SiCepat")

    log.info("⏳ Listening for chain events...")

    try:
        for f in futures:
            f.result()
    except KeyboardInterrupt:
        log.info("🛑 Shutting down filtered subscriber...")
        for f in futures:
            f.cancel()

if __name__ == "__main__":
    main()
