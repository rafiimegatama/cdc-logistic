import os
import logging
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET")
client     = bigquery.Client(project=PROJECT_ID)

PROCEDURES = [
    "bigquery/procedures/sp_deduplicate_orders.sql",
    "bigquery/procedures/sp_deduplicate_shipments.sql",
    "bigquery/procedures/sp_delivery_kpi.sql",
    "bigquery/procedures/sp_order_summary.sql",
]

EXECUTE_ORDER = [
    "sp_deduplicate_orders",
    "sp_deduplicate_shipments",
    "sp_order_summary",
    "sp_delivery_kpi",
]

def deploy_procedure(sql_path: str):
    with open(sql_path, "r") as f:
        sql = f.read()
    job = client.query(sql)
    job.result()
    proc_name = sql_path.split("/")[-1].replace(".sql", "")
    log.info(f"✅ Deployed procedure: {proc_name}")

def run_procedure(proc_name: str):
    sql  = f"CALL `{PROJECT_ID}.{DATASET_ID}.{proc_name}`()"
    log.info(f"▶️  Running {proc_name}...")
    job  = client.query(sql)
    rows = list(job.result())
    for row in rows:
        log.info(f"   {dict(row)}")

def deploy_all():
    log.info("🚀 Deploying all stored procedures...")
    for path in PROCEDURES:
        try:
            deploy_procedure(path)
        except Exception as e:
            log.error(f"❌ Failed to deploy {path}: {e}")

def run_all():
    log.info("▶️  Running all stored procedures...")
    for proc in EXECUTE_ORDER:
        try:
            run_procedure(proc)
        except Exception as e:
            log.error(f"❌ Failed to run {proc}: {e}")

def show_mart_summary():
    log.info("📊 Mart table summary:")
    tables = [
        "mart_orders",
        "mart_shipments",
        "mart_order_summary",
        "mart_delivery_kpi"
    ]
    for tbl in tables:
        try:
            sql  = f"SELECT COUNT(*) AS cnt FROM `{PROJECT_ID}.{DATASET_ID}.{tbl}`"
            rows = list(client.query(sql).result())
            cnt  = rows[0]["cnt"]
            log.info(f"   {tbl:30s} → {cnt} rows")
        except Exception as e:
            log.error(f"   ❌ {tbl}: {e}")

if __name__ == "__main__":
    deploy_all()
    run_all()
    show_mart_summary()
