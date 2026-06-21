-- Enable logical replication for CDC
ALTER SYSTEM SET wal_level = logical;

-- Create schema
CREATE SCHEMA IF NOT EXISTS logistics;

-- ==========================================
-- TABLE: customers
-- ==========================================
CREATE TABLE IF NOT EXISTS logistics.customers (
    customer_id     SERIAL PRIMARY KEY,
    full_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(100) UNIQUE NOT NULL,
    phone           VARCHAR(20),
    city            VARCHAR(50),
    province        VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- TABLE: products
-- ==========================================
CREATE TABLE IF NOT EXISTS logistics.products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(150) NOT NULL,
    category        VARCHAR(50),
    weight_kg       NUMERIC(6,2),
    price           NUMERIC(12,2),
    stock           INT DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- TABLE: orders
-- ==========================================
CREATE TABLE IF NOT EXISTS logistics.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT REFERENCES logistics.customers(customer_id),
    order_status    VARCHAR(30) DEFAULT 'PENDING',
    total_amount    NUMERIC(14,2),
    payment_method  VARCHAR(30),
    order_date      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- TABLE: order_items
-- ==========================================
CREATE TABLE IF NOT EXISTS logistics.order_items (
    item_id         SERIAL PRIMARY KEY,
    order_id        INT REFERENCES logistics.orders(order_id),
    product_id      INT REFERENCES logistics.products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12,2),
    subtotal        NUMERIC(14,2)
);

-- ==========================================
-- TABLE: shipments
-- ==========================================
CREATE TABLE IF NOT EXISTS logistics.shipments (
    shipment_id     SERIAL PRIMARY KEY,
    order_id        INT REFERENCES logistics.orders(order_id),
    courier         VARCHAR(50),
    tracking_number VARCHAR(50) UNIQUE,
    origin_city     VARCHAR(50),
    destination_city VARCHAR(50),
    shipment_status VARCHAR(30) DEFAULT 'WAITING_PICKUP',
    shipped_at      TIMESTAMP,
    estimated_arrival TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- TABLE: delivery_events
-- ==========================================
CREATE TABLE IF NOT EXISTS logistics.delivery_events (
    event_id        SERIAL PRIMARY KEY,
    shipment_id     INT REFERENCES logistics.shipments(shipment_id),
    event_type      VARCHAR(50),
    event_location  VARCHAR(100),
    event_note      VARCHAR(255),
    event_time      TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- PUBLICATION for Debezium CDC
-- ==========================================
CREATE PUBLICATION logistics_pub FOR TABLE
    logistics.customers,
    logistics.products,
    logistics.orders,
    logistics.order_items,
    logistics.shipments,
    logistics.delivery_events;
