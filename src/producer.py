# src/producer.py
import os
import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer as KP
from kafka.errors import KafkaError as KE
from simulation import get_traffic_and_supply as gts, calculate_surge as cs

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
    'bootstrap_servers': ['localhost:9092'], # Update to ['kafka:29092'] if running internally inside a Docker network
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'acks': 'all',
    'retries': 5,
    'max_in_flight_requests_per_connection': 1
}

try:
    logging.info("Initializing Kafka Producer link...")
    producer = KP(**producer_config)
    logging.info("Producer online. Broadcasting live ride streams...")
except KE as e:
    logging.error(f"Failed to connect to Kafka broker: {e}")
    exit(1)

# Live Streaming Loop Configuration
cities_pool = list(locations.keys())
ride_lifecycle_statuses = ["SEARCHING", "ACCEPTED", "ARRIVED", "EN_ROUTE"]

try:
    while True:
        city = random.choice(cities_pool)
        if not locations[city]:
            continue
        area = random.choice(locations[city])

        # Get actual system time to calculate traffic and pricing context
        now = datetime.now()
        decimal_hour = now.hour + (now.minute / 60.0)

        # Simulate environmental factors via extracted logic module
        traffic_mult, demand, supply = gts(decimal_hour)
        
        # Calculate dynamic surge pricing metrics via extracted logic module
        surge_multiplier = cs(demand, supply)

        # Derive distance-based core fare metrics altered by real-world constraints
        base_distance = round(random.uniform(1.5, 15.0), 2)  # Simulated trip distance in miles
        estimated_duration_mins = round((base_distance * 2) * traffic_mult, 1)
        
        # Base Fare Math Strategy: $2.50 flat drop fee + $1.50 per mile, adjusted by surge multiplier
        calculated_fare = round((2.50 + (base_distance * 1.50)) * surge_multiplier, 2)

        # Generate unique tracking identifiers for the event
        current_ride_id = random.randint(100000, 999999)
        current_driver_id = random.randint(1001, 9999)
        current_user_id = random.randint(50001, 59999)

        # Emit state tracking updates sequentially down the pipeline stream
        for status in ride_lifecycle_statuses:
            
            data = {
                "ride_id": current_ride_id,
                "user_id": current_user_id,
                "driver_id": current_driver_id if status != "SEARCHING" else None,
                "location": f"{city}",
                "area": f"{area}",
                "base_distance_miles": base_distance,
                "estimated_duration_minutes": estimated_duration_mins,
                "traffic_delay_multiplier": traffic_mult,
                "simulated_demand_score": demand,
                "simulated_supply_score": supply,
                "surge_multiplier": surge_multiplier,
                "fare": calculated_fare,
                "status": status,
                "timestamp": int(time.time())
            }

            future = producer.send("rides", value=data)
            record_metadata = future.get(timeout=10)
            
            logging.info(
                f"Ride #{data['ride_id']} [{data['status']}] -> {data['location']} ({data['area']}) | "
                f"Fare: ${data['fare']} (Surge: {data['surge_multiplier']}x) | "
                f"Duration: {data['estimated_duration_minutes']} mins"
            )

            # Sub-second pause between dispatch state sequences (mimics driver interaction times)
            time.sleep(0.4)

        # Clear space window before initiating the next ride request event loop
        time.sleep(2)

except KeyboardInterrupt:
    logging.info("\nShutdown signal captured. Halting transmission generator...")
except KE as k_err:
    logging.error(f"Transmit pipeline broken by Broker error: {k_err}")
finally:
    if 'producer' in locals():
        logging.info("Flushing memory cache buffers...")
        producer.flush()
        producer.close()
        logging.info("Producer link offline.")