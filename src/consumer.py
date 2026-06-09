# src/consumer.py
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer as KC
from kafka.errors import KafkaError as KE
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError

# Setup clean logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# CONFIGURATIONS 
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'], # Update to ['kafka:29092'] if running internally inside a Docker network
    'group_id': 'snowflake-ingest-group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
}

BATCH_SIZE = 100
BATCH_TIMEOUT = 10  # Seconds to wait before forcing a flush of a partial batch

# INITIALIZATION 
logging.info("Initializing Kafka Consumer and Snowflake Connection...")

# Using consumer.poll() pattern via consumer assignment rather than basic blocking iterators
consumer = KC('rides', **KAFKA_CONFIG)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cursor = conn.cursor()

cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")

# Phase 5: Expanded Table Schema to accommodate Advanced Simulation Metrics
cursor.execute("""
CREATE TABLE IF NOT EXISTS rides (
    ride_id INT,
    user_id INT,
    driver_id INT,
    location STRING,
    area STRING,
    base_distance_miles FLOAT,
    estimated_duration_minutes FLOAT,
    traffic_delay_multiplier FLOAT,
    simulated_demand_score INT,
    simulated_supply_score INT,
    surge_multiplier FLOAT,
    fare FLOAT,
    status STRING,
    timestamp TIMESTAMP,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

def flush_to_snowflake(buffer_data):
    if not buffer_data:
        return True
        
    query = """
        INSERT INTO rides (
            ride_id, user_id, driver_id, location, area, 
            base_distance_miles, estimated_duration_minutes, traffic_delay_multiplier,
            simulated_demand_score, simulated_supply_score, surge_multiplier, 
            fare, status, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        logging.info(f"Uploading batch of {len(buffer_data)} records to Snowflake...")
        cursor.executemany(query, buffer_data)
        conn.commit()
        logging.info("Batch saved successfully.")
        return True
    except SnowflakeError as db_err:
        logging.error(f"Snowflake Database Error: {db_err}")
        conn.rollback()
        return False

# MAIN STREAMING LOOP
msg_buffer = []
last_flush_time = datetime.now()

logging.info("Streaming Pipeline Active. Kafka → Snowflake (Phase 5 Connected)...")

try:
    while True:
        # poll() prevents the script from blocking indefinitely, checking every 1000ms
        records = consumer.poll(timeout_ms=1000)
        
        for topic_partition, consumer_records in records.items():
            for message in consumer_records:
                if message.value is None:
                    continue

                data = message.value
                logging.info(f"Received metadata from Kafka: Ride #{data['ride_id']} [{data['status']}]")
                
                row_timestamp = data.get("timestamp", datetime.utcnow().timestamp())
                formatted_ts = datetime.utcfromtimestamp(row_timestamp)

                # Map every new payload element perfectly down the relational table tuple parameters
                row = (
                    data["ride_id"],
                    data["user_id"],
                    data.get("driver_id"), # Handles integer or JSON null safely
                    data["location"],
                    data.get("area", "Unknown"),
                    data.get("base_distance_miles", 0.0),
                    data.get("estimated_duration_minutes", 0.0),
                    data.get("traffic_delay_multiplier", 1.0),
                    data.get("simulated_demand_score", 0),
                    data.get("simulated_supply_score", 0),
                    data.get("surge_multiplier", 1.0),
                    data["fare"],
                    data.get("status", "UNKNOWN"),
                    formatted_ts
                )
                msg_buffer.append(row)

        # Evaluate constraints dynamically, even if no new records hit the consumer queue
        time_delta = (datetime.now() - last_flush_time).total_seconds()
        
        if len(msg_buffer) >= BATCH_SIZE or (time_delta >= BATCH_TIMEOUT and msg_buffer):
            if flush_to_snowflake(msg_buffer):
                consumer.commit()
                msg_buffer.clear()
                last_flush_time = datetime.now()
            else:
                logging.warning("Flush failed. Buffer retained to retry on next poll interval.")

except KeyboardInterrupt:
    logging.info("Shutdown sequence initiated by user...")
    if msg_buffer:
        if flush_to_snowflake(msg_buffer):
            consumer.commit()
except KE as k_err:
    logging.error(f"Critical Kafka Failure: {k_err}")
finally:
    cursor.close()
    conn.close()
    consumer.close()
    logging.info("Pipeline connections safely closed.")