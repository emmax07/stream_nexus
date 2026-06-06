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

## Future Improvements

Add Apache Airflow orchestration
Add data validation layer
Add monitoring & logging
Build Power BI dashboard
Add surge pricing simulation
