import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def verify_load():

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM rides
    """)

    count = cursor.fetchone()[0]

    print(
        f"Rows in Snowflake: {count}"
    )

    cursor.close()
    conn.close()