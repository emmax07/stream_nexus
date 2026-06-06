import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# --- CONFIGURATIONS ---
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'group_id': 'snowflake-ingest-group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
}

BATCH_SIZE = 100
BATCH_TIMEOUT = 10

# --- INITIALIZATION ---
logging.info("Initializing Kafka Consumer and Snowflake Connection...")
consumer = KafkaConsumer('rides', **KAFKA_CONFIG)

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

cursor.execute("""
CREATE TABLE IF NOT EXISTS rides (
    ride_id INT,
    user_id INT,
    location STRING,
    area STRING,
    fare FLOAT,
    timestamp TIMESTAMP
)
""")
conn.commit()

def flush_to_snowflake(buffer_data):
    if not buffer_data:
        return True
        
    query = """
        INSERT INTO rides (ride_id, user_id, location, area, fare, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
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

# --- MAIN STREAMING LOOP ---
msg_buffer = []
last_flush_time = datetime.now()

logging.info("Streaming Pipeline Active. Kafka → Snowflake...")

try:
    for message in consumer:
        if message.value is None:
            continue

        data = message.value
        logging.info(f"Received from Kafka: {data['location']} -> {data.get('area')}")
        
        row_timestamp = data.get("timestamp", datetime.utcnow().timestamp())
        formatted_ts = datetime.utcfromtimestamp(row_timestamp)

        # Extract data["area"] cleanly and fall back to None if missing
        row = (
            data["ride_id"],
            data["user_id"],
            data["location"],
            data.get("area", "Unknown"), 
            data["fare"],
            formatted_ts
        )
        msg_buffer.append(row)

        time_delta = (datetime.now() - last_flush_time).total_seconds()
        
        if len(msg_buffer) >= BATCH_SIZE or time_delta >= BATCH_TIMEOUT:
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
except KafkaError as k_err:
    logging.error(f"Critical Kafka Failure: {k_err}")
finally:
    cursor.close()
    conn.close()
    consumer.close()
    logging.info("Pipeline connections safely closed.")