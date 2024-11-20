from config import load_config, logger
import psycopg2
from psycopg2 import pool
import json


def get_db_connection(postgresql_config):
    conn = psycopg2.connect(
        dbname=postgresql_config["dbname"],
        user=postgresql_config["user"],
        password=postgresql_config["password"],
        host=postgresql_config["host"],
        port=postgresql_config["port"]
    )
    return conn


def store_message_in_db(conn,message, timestamp, kafka_offset):
    """Store Kafka messages in the PostgreSQL database."""
    cursor = conn.cursor()
    
    try:
        if not isinstance(message, str):
            message = message.decode('utf-8')
        # Use ON CONFLICT to avoid duplicate messages based on the kafka_offset
        cursor.execute("""
            INSERT INTO kafka_data (message, timestamp, kafka_offset)
            VALUES (%s, to_timestamp(%s), %s)
            ON CONFLICT (kafka_offset) DO NOTHING
        """, (message, timestamp, kafka_offset))
        conn.commit()
        logger.info(f"Message stored successfully: Offset={kafka_offset}, Message={message}")
    except Exception as e:
        logger.error(f"Failed to store message: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
