"""
Flink Job 2: Delivery SLA Breach Detection
Reads from Kafka cdc.logistics.delivery_events
Detects shipments breaching SLA using sliding windows
Writes alerts to BigQuery flink_delivery_sla table
SLA: shipment should complete within 10 delivery events
     or transit > 30 minutes between events = breach
"""
import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv("/opt/flink/.env")

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    MapFunction,
    FilterFunction
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("delivery_sla")

PROJECT_ID      = os.getenv("GCP_PROJECT_ID")
DATASET_ID      = os.getenv("BIGQUERY_DATASET", "logistics_dwh")
TABLE_ID        = "flink_delivery_sla"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

SLA_MAX_EVENTS   = 10
SLA_MAX_MINUTES  = 120

# ==========================================
# PARSE DELIVERY EVENT
# ==========================================
class ParseDeliveryEvent(MapFunction):
    def map(self, value):
        try:
            msg     = json.loads(value)
            payload = msg.get("payload", {})
            op      = payload.get("op")
            after   = payload.get("after")

            if not after or op not in ("c", "r"):
                return None

            return json.dumps({
                "event_id":       after.get("event_id"),
                "shipment_id":    after.get("shipment_id"),
                "event_type":     after.get("event_type", ""),
                "event_location": after.get("event_location", ""),
                "event_time":     int(after.get("event_time") or 0) // 1000,
            })
        except Exception:
            return None

class FilterValidEvents(FilterFunction):
    def filter(self, value):
        if not value:
            return False
        try:
            d = json.loads(value)
            return d.get("shipment_id") is not None
        except Exception:
            return False

# ==========================================
# SLA WINDOW PROCESSOR
# ==========================================
class SLAWindowProcessor(ProcessWindowFunction):

    def _get_bq_client(self):
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)

    def process(self, key, context, elements):
        events_list  = [json.loads(e) for e in elements]
        shipment_id  = key
        event_count  = len(events_list)
        event_types  = [e.get("event_type", "") for e in events_list]
        is_delivered = "DELIVERED" in event_types
        is_failed    = "FAILED" in event_types

        event_times = sorted([
            e.get("event_time", 0) for e in events_list
            if e.get("event_time", 0) > 0
        ])

        transit_minutes = 0.0
        if len(event_times) >= 2:
            transit_minutes = (event_times[-1] - event_times[0]) / 60.0

        sla_breached = (
            event_count > SLA_MAX_EVENTS or
            (transit_minutes > SLA_MAX_MINUTES and not is_delivered) or
            is_failed
        )

        window_start = datetime.fromtimestamp(
            context.window().start / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        window_end = datetime.fromtimestamp(
            context.window().end / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "window_start":    window_start,
            "window_end":      window_end,
            "shipment_id":     shipment_id,
            "order_id":        events_list[0].get("shipment_id"),
            "courier":         None,
            "sla_breached":    sla_breached,
            "transit_minutes": round(transit_minutes, 2),
            "event_count":     event_count,
            "processed_at":    datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        }

        status = "🚨 BREACH" if sla_breached else "✅ OK"
        log.info(
            f"{status} | shipment={shipment_id} | "
            f"events={event_count} transit={transit_minutes:.1f}min"
        )

        try:
            bq     = self._get_bq_client()
            table  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = bq.insert_rows_json(table, [result])
            if errors:
                log.error(f"BQ error: {errors}")
            else:
                log.info(f"✅ Written to BQ {TABLE_ID}")
        except Exception as e:
            log.error(f"BQ write failed: {e}")

        yield json.dumps(result)

# ==========================================
# TIMESTAMP ASSIGNER
# ==========================================
class DeliveryTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            d = json.loads(value)
            return int(d.get("event_time", 0)) * 1000
        except Exception:
            return 0

# ==========================================
# MAIN
# ==========================================
def main():
    log.info("🚀 Starting Flink Delivery SLA Job")
    log.info(f"   Project    : {PROJECT_ID}")
    log.info(f"   SLA events : max {SLA_MAX_EVENTS} events per window")
    log.info(f"   SLA time   : max {SLA_MAX_MINUTES} minutes transit")
    log.info(f"   Window     : 10-minute tumbling per shipment")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics("cdc.logistics.delivery_events") \
        .set_group_id("flink-delivery-sla") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(30)) \
        .with_timestamp_assigner(DeliveryTimestampAssigner())

    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka Delivery Events Source"
    )

    result = stream \
        .map(ParseDeliveryEvent()) \
        .filter(FilterValidEvents()) \
        .key_by(lambda x: str(json.loads(x).get("shipment_id", "unknown"))) \
        .window(TumblingEventTimeWindows.of(Time.minutes(10))) \
        .process(SLAWindowProcessor())

    result.print()

    log.info("▶️  Submitting Flink SLA job...")
    env.execute("CDC Logistics — Delivery SLA Detection 10min")

if __name__ == "__main__":
    main()
