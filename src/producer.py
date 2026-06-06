import os
import json
import time
import random
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup clean logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load External File Safely
FILE_PATH = "data/locations.json"

if not os.path.exists(FILE_PATH):
    logging.error(f"Initialization aborted: '{FILE_PATH}' file not found.")
    exit(1)

try:
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        locations = json.load(f)
    if not isinstance(locations, dict) or not locations:
        raise ValueError("JSON root structure must be a non-empty object map.")
    logging.info(f"Successfully loaded {len(locations)} cities from {FILE_PATH}.")
except Exception as e:
    logging.error(f"Failed to parse '{FILE_PATH}': {e}")
    exit(1)

# Kafka Producer Setup
producer_config = {
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'acks': 'all',
    'retries': 5,
    'max_in_flight_requests_per_connection': 1
}

try:
    logging.info("Initializing Kafka Producer link...")
    producer = KafkaProducer(**producer_config)
    logging.info("Producer online. Broadcasting live ride streams...")
except KafkaError as e:
    logging.error(f"Failed to connect to Kafka broker: {e}")
    exit(1)

# Live Streaming Loop
cities_pool = list(locations.keys())

try:
    while True:
        city = random.choice(cities_pool)
        if not locations[city]:
            continue
        area = random.choice(locations[city])

        # PERFECTLY MATCHED DATA: area belongs directly to city
        data = {
            "ride_id": random.randint(1, 1000),
            "user_id": random.randint(1, 100),
            "location": f"{city}",
            "area": f"{area}",
            "fare": round(random.uniform(5, 50), 2),
            "timestamp": int(time.time())
        }

        future = producer.send("rides", value=data)
        record_metadata = future.get(timeout=10)
        
        # Show the perfectly matched city and area
        logging.info(f"Sent: {data['location']} ({data['area']}) | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

        time.sleep(2)

except KeyboardInterrupt:
    logging.info("\nShutdown signal captured. Halting transmission generator...")
except KafkaError as k_err:
    logging.error(f"Transmit pipeline broken by Broker error: {k_err}")
finally:
    if 'producer' in locals():
        logging.info("Flushing memory cache buffers...")
        producer.flush()
        producer.close()
        logging.info("Producer link offline.")