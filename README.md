# Real-Time Ride Streaming Data Pipeline

## Overview

This project is a **real-time data engineering pipeline** that simulates ride-sharing data and streams it through a modern data stack:

**Python → Kafka → Snowflake**

It demonstrates end-to-end skills in:

- Real-time streaming (Kafka)
- Data generation (Python)
- Cloud data warehousing (Snowflake)
- ETL pipeline design
- Environment-based configuration
- Production-style project structure

---

## Architecture

Producer (Python)
↓
Apache Kafka (Docker)
↓
Consumer (Python)
↓
Snowflake Data Warehouse

---

## Tech Stack

- Python 3.10+
- Apache Kafka (Docker)
- Snowflake Cloud Data Warehouse
- Docker Compose
- JSON Configuration
- dotenv (.env secrets management)

---

## Project Structure

stream_nexus/
│
├──data/
├──├── locations.json
├──src/
├──├── producer.py
├──├── consumer.py
├──airflow/
│──├── docker-compose.yml
│──├── dags/
│──├──├── check_snowflake.py
│──├──├── ride_pipeline.py
│──├──├── test_sync.py
│──└──└── check_kafka.py
├── docker-compose.yml
├── .env
├── requirements.txt
└── README.md

---

## Features

- Real-time streaming data pipeline
- Multi-city ride simulation
- Randomized ride generation (fare, location, users)
- Kafka message streaming
- Snowflake data storage
- Secure credential handling using `.env`
- External configuration using JSON

---

## Data Model

Each ride event contains:

{
"ride_id": 123,
"user_id": 45,
"location": "NYC",
"area": "Brooklyn"
"fare": 25.50,
"timestamp": 1720000000
}

---

## Setup Instructions

1. Clone project
   git clone <repo-url>
   cd data-project

2. Create virtual environment
   python -m venv venv
   venv\Scripts\activate

3. Install dependencies
   pip install -r requirements.txt

4. Start Kafka (Docker)
   docker compose up -d

5. Create Snowflake objects

Run in Snowflake:

CREATE DATABASE RIDE_DB;
USE DATABASE RIDE_DB;

CREATE SCHEMA PUBLIC;
USE SCHEMA PUBLIC;

CREATE TABLE rides (
ride_id INT,
user_id INT,
location STRING,
fare FLOAT,
timestamp TIMESTAMP
);

6. Run Producer
   python producer.py

7. Run Consumer
   python consumer.py

## Environment Variables

Create a .env file:

SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RIDE_DB
SNOWFLAKE_SCHEMA=PUBLIC

---

## What I Learn From This Project

Real-time data streaming
Kafka message processing
ETL pipeline design
Cloud data warehouse integration
Data engineering best practices
Modular project structure

## Improvements Done

The core streaming engine has been re-architected from a basic, single-track scripting setup into a highly decoupled, production-grade streaming framework:

# Advanced Market Simulation:

Re-engineered the payload engine to compute real-time simulation math (`src/simulation.py`). The system now tracks computer clock cycles to dynamically generate rush-hour traffic vectors and applies an exponential supply-to-demand surge pricing matrix alongside a sequential 4-state ride lifecycle (`SEARCHING` → `ACCEPTED` → `ARRIVED` → `EN_ROUTE`).

# Non-Blocking Consumer Core:

Shifted the ingestion layer to a `consumer.poll()` pattern. This prevents thread lock, isolates micro-batches, and enforces a mandatory 10-second data flush mechanism to guarantee downstream consistency in Snowflake regardless of Kafka traffic volume.

# Production-Grade Airflow Orchestration:

Eliminated the infinite-loop subprocess block. Airflow has been promoted to a non-blocking Infrastructure Auditor & Monitor that wakes up `@hourly` to probe broker health and validate Snowflake table accumulation, backed by automated retry and error-recovery policies.

# Real-Time Operations Analytics:

Expanded the target Snowflake warehouse schema to a 15-dimensional matrix. Upgraded the Streamlit control tower with an auto-refresh engine, live operational scorecards (Gross Revenue, Max Surge Factors), and scatter plots mapping price volatility directly against traffic delay vectors.
