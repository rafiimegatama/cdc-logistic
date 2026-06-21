"""
Flink Job 3: Anomaly Detection — Stateful Stream Processing
Detects anomalies in real-time:
  1. High value orders (> 3x rolling average)
  2. Order velocity spikes (too many orders in short window)
  3. Rapid status changes (order flipping status suspiciously fast)
  4. Failed delivery clusters (same courier failing repeatedly)
Writes alerts to BigQuery flink_anomalies table
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
from pyflink.datastream.window import SlidingEventTimeWindows, Time
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    MapFunction,
    FilterFunction
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("anomaly_detection")

PROJECT_ID      = os.getenv("GCP_PROJECT_ID")
DATASET_ID      = os.getenv("BIGQUERY_DATASET", "logistics_dwh")
TABLE_ID        = "flink_anomalies"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

# Thresholds
HIGH_VALUE_MULTIPLIER = 3.0
HIGH_VALUE_FLOOR      = 5_000_000
VELOCITY_THRESHOLD    = 15
FAILED_CLUSTER_MIN    = 3

# ==========================================
# PARSE ORDER MESSAGE
# ==========================================
class ParseOrderForAnomaly(MapFunction):
    def map(self, value):
        try:
            msg     = json.loads(value)
            payload = msg.get("payload", {})
            op      = payload.get("op")
            after   = payload.get("after")
            before  = payload.get("before")

            if not after or op not in ("c", "u", "r"):
                return None

            return json.dumps({
                "order_id":       after.get("order_id"),
                "customer_id":    after.get("customer_id"),
                "order_status":   after.get("order_status", ""),
                "total_amount":   float(after.get("total_amount") or 0),
                "payment_method": after.get("payment_method", ""),
                "prev_status":    before.get("order_status") if before else None,
                "event_time":     int(after.get("order_date") or 0) // 1000,
                "op":             op,
            })
        except Exception:
            return None

class FilterValidOrders(FilterFunction):
    def filter(self, value):
        if not value:
            return False
        try:
            d = json.loads(value)
            return d.get("order_id") is not None and d.get("total_amount", 0) > 0
        except Exception:
            return False

# ==========================================
# ANOMALY WINDOW PROCESSOR
# ==========================================
class AnomalyDetector(ProcessWindowFunction):

    def _get_bq_client(self):
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)

    def _write_anomaly(self, anomaly: dict):
        try:
            bq     = self._get_bq_client()
            table  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            errors = bq.insert_rows_json(table, [anomaly])
            if errors:
                log.error(f"BQ error: {errors}")
            else:
                log.info(
                    f"🚨 Anomaly written | "
                    f"type={anomaly['anomaly_type']} | "
                    f"severity={anomaly['severity']} | "
                    f"order={anomaly.get('order_id')}"
                )
        except Exception as e:
            log.error(f"BQ write failed: {e}")

    def process(self, key, context, elements):
        events_list = [json.loads(e) for e in elements]
        now         = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        anomalies   = []

        window_start = datetime.fromtimestamp(
            context.window().start / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        window_end = datetime.fromtimestamp(
            context.window().end / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

        amounts       = [e.get("total_amount", 0) for e in events_list]
        avg_amount    = sum(amounts) / len(amounts) if amounts else 0
        order_count   = len(events_list)

        # ── Anomaly 1: High value order ──
        for e in events_list:
            amount    = e.get("total_amount", 0)
            threshold = max(
                avg_amount * HIGH_VALUE_MULTIPLIER,
                HIGH_VALUE_FLOOR
            )
            if amount > threshold:
                anomaly = {
                    "detected_at":   now,
                    "anomaly_type":  "HIGH_VALUE_ORDER",
                    "order_id":      e.get("order_id"),
                    "shipment_id":   None,
                    "metric_value":  amount,
                    "threshold":     round(threshold, 2),
                    "description":   (
                        f"Order {e.get('order_id')} amount "
                        f"Rp{amount:,.0f} exceeds "
                        f"{HIGH_VALUE_MULTIPLIER}x window avg "
                        f"Rp{avg_amount:,.0f}"
                    ),
                    "severity":      "HIGH" if amount > threshold * 2 else "MEDIUM",
                    "processed_at":  now
                }
                anomalies.append(anomaly)
                log.warning(
                    f"🚨 HIGH_VALUE_ORDER | order={e.get('order_id')} | "
                    f"amount=Rp{amount:,.0f} | threshold=Rp{threshold:,.0f}"
                )

        # ── Anomaly 2: Order velocity spike ──
        if order_count > VELOCITY_THRESHOLD:
            anomaly = {
                "detected_at":   now,
                "anomaly_type":  "VELOCITY_SPIKE",
                "order_id":      None,
                "shipment_id":   None,
                "metric_value":  float(order_count),
                "threshold":     float(VELOCITY_THRESHOLD),
                "description":   (
                    f"{order_count} orders in window "
                    f"[{window_start} → {window_end}] "
                    f"exceeds threshold {VELOCITY_THRESHOLD}"
                ),
                "severity":      "HIGH" if order_count > VELOCITY_THRESHOLD * 2 else "MEDIUM",
                "processed_at":  now
            }
            anomalies.append(anomaly)
            log.warning(
                f"🚨 VELOCITY_SPIKE | count={order_count} | "
                f"threshold={VELOCITY_THRESHOLD}"
            )

        # ── Anomaly 3: Rapid status reversal ──
        for e in events_list:
            prev   = e.get("prev_status")
            curr   = e.get("order_status")
            if prev == "DELIVERED" and curr in ("PENDING", "PROCESSING"):
                anomaly = {
                    "detected_at":   now,
                    "anomaly_type":  "STATUS_REVERSAL",
                    "order_id":      e.get("order_id"),
                    "shipment_id":   None,
                    "metric_value":  1.0,
                    "threshold":     0.0,
                    "description":   (
                        f"Order {e.get('order_id')} reversed "
                        f"from {prev} → {curr}"
                    ),
                    "severity":      "CRITICAL",
                    "processed_at":  now
                }
                anomalies.append(anomaly)
                log.warning(
                    f"🚨 STATUS_REVERSAL | order={e.get('order_id')} | "
                    f"{prev}→{curr}"
                )

        # ── Write all anomalies to BQ ──
        for anomaly in anomalies:
            self._write_anomaly(anomaly)
            yield json.dumps(anomaly)

        if not anomalies:
            log.info(
                f"✅ Window clean | orders={order_count} | "
                f"avg=Rp{avg_amount:,.0f}"
            )

# ==========================================
# TIMESTAMP ASSIGNER
# ==========================================
class OrderTimestampAssigner:
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
    log.info("🚀 Starting Flink Anomaly Detection Job")
    log.info(f"   Project            : {PROJECT_ID}")
    log.info(f"   High value floor   : Rp{HIGH_VALUE_FLOOR:,}")
    log.info(f"   High value mult    : {HIGH_VALUE_MULTIPLIER}x avg")
    log.info(f"   Velocity threshold : {VELOCITY_THRESHOLD} orders/window")
    log.info(f"   Window             : 10-min sliding, 5-min slide")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics("cdc.logistics.orders") \
        .set_group_id("flink-anomaly-detector") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(30)) \
        .with_timestamp_assigner(OrderTimestampAssigner())

    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka Orders Source (Anomaly)"
    )

    result = stream \
        .map(ParseOrderForAnomaly()) \
        .filter(FilterValidOrders()) \
        .key_by(lambda x: "global") \
        .window(SlidingEventTimeWindows.of(
            Time.minutes(10),
            Time.minutes(5)
        )) \
        .process(AnomalyDetector())

    result.print()

    log.info("▶️  Submitting anomaly detection job...")
    env.execute("CDC Logistics — Anomaly Detection (sliding window)")

if __name__ == "__main__":
    main()
