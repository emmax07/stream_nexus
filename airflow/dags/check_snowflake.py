import os
import logging
from dotenv import load_dotenv
import snowflake.connector

# Explicitly pull env variables
load_dotenv()

def verify_load():
    logging.info("Establishing auditing link to Snowflake warehouse...")
    
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    logging.info("Querying stream ingest table volume metadata...")
    cursor.execute("SELECT COUNT(*) FROM rides")
    count = cursor.fetchone()[0]

    logging.info(f"Audit Result -> Total rows found in Snowflake table: {count}")

    cursor.close()
    conn.close()

    # Data Validation Guardrail Layer
    if count == 0:
        raise ValueError(
            "Validation Alert: The 'rides' table is entirely empty! "
            "Your consumer script might not be processing or saving Kafka payloads."
        )
        
    logging.info("Validation Success: Data pipeline records are successfully accumulating in Cloud Data Warehouse.")