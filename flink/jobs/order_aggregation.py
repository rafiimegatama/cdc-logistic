"""
Flink Job 1: Order Aggregation with Tumbling Windows
Reads from Kafka cdc.logistics.orders topic
Aggregates per 5-minute tumbling window → BigQuery
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
log = logging.getLogger("order_aggregation")

PROJECT_ID      = os.getenv("GCP_PROJECT_ID")
DATASET_ID      = os.getenv("BIGQUERY_DATASET", "logistics_dwh")
TABLE_ID        = "flink_order_agg"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

# ==========================================
# PARSE DEBEZIUM MESSAGE
# ==========================================
class ParseOrderMessage(MapFunction):
    def map(self, value):
        try:
            msg     = json.loads(value)
            payload = msg.get("payload", {})
            op      = payload.get("op")
            after   = payload.get("after")

            if not after or op not in ("c", "u", "r"):
                return None

            return json.dumps({
                "order_id":       after.get("order_id"),
                "order_status":   after.get("order_status", ""),
                "total_amount":   float(after.get("total_amount") or 0),
                "payment_method": after.get("payment_method", ""),
                "event_time":     int(after.get("order_date") or 0) // 1000,
            })
        except Exception as e:
            return None

class FilterValidOrders(FilterFunction):
    def filter(self, value):
        if not value:
            return False
        try:
            d = json.loads(value)
            return d.get("order_id") is not None
        except Exception:
            return False

# ==========================================
# WINDOW AGGREGATION
# ==========================================
class OrderWindowAggregator(ProcessWindowFunction):
    """
    Flink ProcessWindowFunction — BQ client created per process()
    to avoid pickling issues with google-cloud client objects.
    """

    def _get_bq_client(self):
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)

    def process(self, key, context, elements):
        elements_list = [json.loads(e) for e in elements]
        total_orders  = len(elements_list)

        if total_orders == 0:
            return

        total_revenue  = sum(e.get("total_amount", 0) for e in elements_list)
        avg_order_val  = total_revenue / total_orders

        status_counts  = {}
        payment_counts = {}

        for e in elements_list:
            s = e.get("order_status", "UNKNOWN")
            p = e.get("payment_method", "UNKNOWN")
            status_counts[s]  = status_counts.get(s, 0) + 1
            payment_counts[p] = payment_counts.get(p, 0) + 1

        top_payment = max(payment_counts, key=payment_counts.get) \
            if payment_counts else "UNKNOWN"

        window_start = datetime.fromtimestamp(
            context.window().start / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        window_end = datetime.fromtimestamp(
            context.window().end / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "window_start":     window_start,
            "window_end":       window_end,
            "total_orders":     total_orders,
            "total_revenue":    round(total_revenue, 2),
            "avg_order_value":  round(avg_order_val, 2),
            "status_pending":   status_counts.get("PENDING", 0),
            "status_shipped":   status_counts.get("SHIPPED", 0),
            "status_delivered": status_counts.get("DELIVERED", 0),
            "top_payment":      top_payment,
            "processed_at":     datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        }

        log.info(
            f"Window [{window_start} → {window_end}] | "
            f"orders={total_orders} revenue=Rp{total_revenue:,.0f} "
            f"top_payment={top_payment}"
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
# WATERMARK TIMESTAMP ASSIGNER
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
    log.info("🚀 Starting Flink Order Aggregation Job")
    log.info(f"   Project : {PROJECT_ID}")
    log.info(f"   Kafka   : {KAFKA_BOOTSTRAP}")
    log.info(f"   Window  : 5-minute tumbling")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics("cdc.logistics.orders") \
        .set_group_id("flink-order-agg") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(30)) \
        .with_timestamp_assigner(OrderTimestampAssigner())

    stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka Orders Source"
    )

    result = stream \
        .map(ParseOrderMessage()) \
        .filter(FilterValidOrders()) \
        .key_by(lambda x: "global") \
        .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
        .process(OrderWindowAggregator())

    result.print()

    log.info("▶️  Submitting Flink job to cluster...")
    env.execute("CDC Logistics — Order Aggregation 5min")

if __name__ == "__main__":
    main()
