# 🚚 CDC Logistics Pipeline — Event-Driven Architecture

Real-time Change Data Capture (CDC) pipeline for logistics e-commerce using PostgreSQL, Kafka, Debezium, Google Pub/Sub, and BigQuery.

## 🏗️ Architecture
## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Source DB | PostgreSQL 14 (WAL logical replication) |
| CDC Engine | Debezium 2.4 |
| Message Broker | Apache Kafka 7.4 |
| Cloud Messaging | Google Cloud Pub/Sub |
| Data Warehouse | Google BigQuery |
| Orchestration | Python (kafka-python, google-cloud-pubsub) |
| Infrastructure | Docker Compose, WSL2 |

## 🚀 Quick Start

See the full setup guide in [SETUP.md](SETUP.md)

## 📦 Domain

Logistics e-commerce simulation with Indonesian cities, couriers (JNE, SiCepat, JNT), and real-time order tracking events.

## 📊 Tables

- `customers` — 20 dummy customers across Indonesian cities
- `products` — 15 products across categories
- `orders` — Real-time order lifecycle (PENDING → DELIVERED)
- `order_items` — Line items per order
- `shipments` — Courier tracking (JNE, SiCepat, JNT, Anteraja, TIKI)
- `delivery_events` — Milestone events (PICKUP → IN_TRANSIT → DELIVERED)

## ⚠️ Environment Setup

Copy `.env.example` to `.env` and fill in your values:
```bash
cp .env.example .env
```
