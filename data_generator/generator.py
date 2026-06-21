import psycopg2
import random
import time
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

log = config.setup_logging("generator")

def get_conn():
    return psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        dbname=config.PG_DB,
        user=config.PG_USER,
        password=config.PG_PASSWORD
    )

COURIERS  = ["JNE", "SiCepat", "JNT", "Anteraja", "TIKI", "Pos Indonesia"]
CITIES    = ["Jakarta", "Surabaya", "Bandung", "Medan", "Yogyakarta",
              "Semarang", "Makassar", "Palembang", "Balikpapan", "Denpasar"]
PAYMENTS  = ["TRANSFER", "CREDIT_CARD", "GOPAY", "OVO", "DANA", "COD"]
EVT_TYPES = ["PICKUP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "FAILED"]

def insert_new_order(cur):
    customer_id    = random.randint(1, 20)
    product_id     = random.randint(1, 15)
    quantity       = random.randint(1, 5)
    unit_price     = random.choice([
        89000, 129000, 189000, 259000, 389000,
        499000, 549000, 1299000, 1899000, 4299000
    ])
    subtotal       = unit_price * quantity
    payment_method = random.choice(PAYMENTS)

    cur.execute("""
        INSERT INTO logistics.orders
            (customer_id, order_status, total_amount, payment_method)
        VALUES (%s, 'PENDING', %s, %s)
        RETURNING order_id
    """, (customer_id, subtotal, payment_method))
    order_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO logistics.order_items
            (order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s)
    """, (order_id, product_id, quantity, unit_price, subtotal))
    log.info(f"🛒 New order #{order_id} | customer={customer_id} | amount={subtotal:,}")

def update_order_status(cur):
    cur.execute("""
        SELECT order_id FROM logistics.orders
        WHERE order_status NOT IN ('DELIVERED','CANCELLED')
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return
    order_id   = row[0]
    new_status = random.choice(["PROCESSING", "SHIPPED", "DELIVERED"])
    cur.execute("""
        UPDATE logistics.orders
        SET order_status=%s, updated_at=NOW()
        WHERE order_id=%s
    """, (new_status, order_id))
    log.info(f"📦 Order #{order_id} → {new_status}")

def insert_shipment(cur):
    cur.execute("""
        SELECT o.order_id FROM logistics.orders o
        LEFT JOIN logistics.shipments s ON s.order_id=o.order_id
        WHERE o.order_status='SHIPPED' AND s.shipment_id IS NULL
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return
    order_id        = row[0]
    courier         = random.choice(COURIERS)
    tracking_number = f"{courier[:3].upper()}{random.randint(10000000,99999999)}"
    origin          = random.choice(CITIES)
    destination     = random.choice(CITIES)
    estimated       = datetime.now() + timedelta(days=random.randint(1, 5))
    cur.execute("""
        INSERT INTO logistics.shipments
            (order_id, courier, tracking_number, origin_city,
             destination_city, shipment_status, shipped_at, estimated_arrival)
        VALUES (%s,%s,%s,%s,%s,'WAITING_PICKUP',NOW(),%s)
        RETURNING shipment_id
    """, (order_id, courier, tracking_number, origin, destination, estimated))
    shipment_id = cur.fetchone()[0]
    log.info(f"🚚 Shipment #{shipment_id} | {courier} | {tracking_number}")

def insert_delivery_event(cur):
    cur.execute("""
        SELECT shipment_id FROM logistics.shipments
        WHERE shipment_status != 'DELIVERED'
        ORDER BY RANDOM() LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        return
    shipment_id = row[0]
    event_type  = random.choice(EVT_TYPES)
    location    = random.choice(CITIES)
    notes = {
        "PICKUP":           "Paket diambil kurir",
        "IN_TRANSIT":       "Paket dalam perjalanan",
        "OUT_FOR_DELIVERY": "Paket sedang diantar",
        "DELIVERED":        "Paket diterima penerima",
        "FAILED":           "Gagal diantar, akan dicoba ulang"
    }
    cur.execute("""
        INSERT INTO logistics.delivery_events
            (shipment_id, event_type, event_location, event_note)
        VALUES (%s,%s,%s,%s)
    """, (shipment_id, event_type, location, notes[event_type]))
    cur.execute("""
        UPDATE logistics.shipments
        SET shipment_status=%s, updated_at=NOW()
        WHERE shipment_id=%s
    """, (event_type, shipment_id))
    log.info(f"📍 Delivery event | shipment=#{shipment_id} | {event_type} @ {location}")

def main():
    log.info("🚀 Starting CDC data generator")
    log.info(f"   DB: {config.PG_USER}@{config.PG_HOST}:{config.PG_PORT}/{config.PG_DB}")
    conn = get_conn()
    conn.autocommit = True
    actions = [
        insert_new_order,
        update_order_status,
        insert_shipment,
        insert_delivery_event,
    ]
    while True:
        try:
            with conn.cursor() as cur:
                random.choice(actions)(cur)
        except Exception as e:
            log.error(f"❌ Generator error: {e}")
            try:
                conn = get_conn()
                conn.autocommit = True
            except Exception as ce:
                log.error(f"❌ Reconnect failed: {ce}")
        interval = random.randint(
            config.GENERATOR_MIN_SLEEP,
            config.GENERATOR_MAX_SLEEP
        )
        log.info(f"⏳ Next event in {interval}s...")
        time.sleep(interval)

if __name__ == "__main__":
    main()
