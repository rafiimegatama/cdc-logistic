import json
import sys
import time
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import pubsub_v1
from google.api_core import gapic_v1

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from schemas.schema_registry import validate_message, check_health

log = config.setup_logging("publisher")

# ── Enable message ordering on publisher client ──
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher         = pubsub_v1.PublisherClient(publisher_options=publisher_options)

metrics = {
    "total_received":  0,
    "total_published": 0,
    "total_rejected":  0,
    "total_errors":    0,
    "by_topic":        {}
}

def log_metrics():
    log.info(
        f"📊 Metrics | "
        f"received={metrics['total_received']} "
        f"published={metrics['total_published']} "
        f"rejected={metrics['total_rejected']} "
        f"errors={metrics['total_errors']} "
        f"by_topic={metrics['by_topic']}"
    )

def parse_debezium_message(raw: dict) -> dict | None:
    try:
        payload = raw.get("payload", {})
        op      = payload.get("op")
        before  = payload.get("before")
        after   = payload.get("after")
        if op is None:
            return None
        op_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "READ"}
        cdc_op = op_map.get(op, op)
        data   = before if cdc_op == "DELETE" else after
        if data is None:
            return None
        return {
            "cdc_operation": cdc_op,
            "cdc_timestamp": datetime.now(timezone.utc).isoformat(),
            "before":        before,
            "after":         after,
            "data":          data
        }
    except Exception as e:
        log.error(f"❌ Parse error: {e}")
        return None

def validate_against_schema(data: dict, kafka_topic: str) -> tuple[bool, list]:
    subject = config.KAFKA_TO_SCHEMA.get(kafka_topic)
    if not subject or data is None:
        return True, []
    data_with_cdc = {
        **data,
        "cdc_operation": "INSERT",
        "cdc_timestamp": datetime.now(timezone.utc).isoformat()
    }
    return validate_message(data_with_cdc, subject)

def publish_to_pubsub(topic_path: str, message: dict, kafka_topic: str) -> bool:
    data         = message.get("data") or {}
    cdc_op       = message.get("cdc_operation", "UNKNOWN")
    order_id     = str(data.get("order_id", "")) if isinstance(data, dict) else ""
    ordering_key = config.build_ordering_key(
        {"order_id": order_id} if order_id else {},
        cdc_op
    )

    attributes = {
        "kafka_topic":    kafka_topic,
        "cdc_operation":  cdc_op,
        "cdc_timestamp":  message.get("cdc_timestamp", ""),
        "schema_version": "1",
        "order_status":   str(data.get("order_status", "")) if isinstance(data, dict) else "",
        "courier":        str(data.get("courier", ""))       if isinstance(data, dict) else "",
        "event_type":     str(data.get("event_type", ""))    if isinstance(data, dict) else "",
    }

    payload = json.dumps(message).encode("utf-8")

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            future = publisher.publish(
                topic_path,
                payload,
                ordering_key=ordering_key,
                **attributes
            )
            msg_id = future.result(timeout=config.PUBLISH_TIMEOUT)
            metrics["total_published"] += 1
            topic_key = kafka_topic.split(".")[-1]
            metrics["by_topic"][topic_key] = \
                metrics["by_topic"].get(topic_key, 0) + 1
            log.info(
                f"✅ Published | topic={topic_key} "
                f"op={cdc_op} key={ordering_key} msg_id={msg_id}"
            )
            return True
        except Exception as e:
            if attempt < config.MAX_RETRIES:
                wait = config.RETRY_BACKOFF_BASE ** attempt
                log.warning(f"⚠️ Retry {attempt}/{config.MAX_RETRIES} in {wait}s: {e}")
                time.sleep(wait)
            else:
                log.error(f"❌ Failed after {config.MAX_RETRIES} attempts: {e}")
                metrics["total_errors"] += 1
                return False

def handle_rejected(data: dict, kafka_topic: str, errors: list):
    metrics["total_rejected"] += 1
    log.warning(
        f"🚫 Rejected | topic={kafka_topic} | "
        f"errors={errors} | data={json.dumps(data)[:200]}"
    )

def main():
    log.info("🚀 Starting Kafka → Pub/Sub bridge with ordering keys")
    log.info(f"   Project  : {config.GCP_PROJECT_ID}")
    log.info(f"   Topics   : {config.KAFKA_TOPICS}")
    log.info(f"   Ordering : enabled")

    if not check_health():
        log.warning("⚠️ Schema Registry unavailable — validation skipped")

    consumer = KafkaConsumer(
        *config.KAFKA_TOPICS,
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=config.KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None
    )

    log.info("✅ Connected to Kafka, waiting for CDC events...")
    msg_count = 0

    for msg in consumer:
        try:
            if msg.value is None:
                continue
            if not isinstance(msg.value, dict):
                continue

            metrics["total_received"] += 1
            msg_count   += 1
            kafka_topic  = msg.topic
            parsed       = parse_debezium_message(msg.value)

            if parsed is None:
                continue

            data = parsed.get("data")
            is_valid, errors = validate_against_schema(data, kafka_topic)
            if not is_valid:
                handle_rejected(data, kafka_topic, errors)
                continue

            full_message = {**parsed, "schema_validated": True, "schema_version": "1"}
            topic_id     = config.KAFKA_TO_PUBSUB.get(kafka_topic)
            topic_path   = publisher.topic_path(config.GCP_PROJECT_ID, topic_id)
            publish_to_pubsub(topic_path, full_message, kafka_topic)

            if msg_count % config.METRICS_LOG_EVERY == 0:
                log_metrics()

        except Exception as e:
            log.error(f"❌ Error processing message: {e}")
            metrics["total_errors"] += 1

if __name__ == "__main__":
    main()
