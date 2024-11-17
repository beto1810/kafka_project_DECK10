#import Library
import configparser
from confluent_kafka import Consumer, KafkaException, KafkaError
import psycopg2
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environmental variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Parsing info from config file
config = configparser.ConfigParser()
config.read("kafka_config.ini")

def load_config():
    # Convert the config file into a dictionary
    config_dict = {section: dict(config.items(section)) for section in config.sections()}

    # Replace environment variables in the username and password fields
    if 'kafka' in config_dict:
        kafka_config = config_dict['kafka']
        kafka_config['sasl.username'] = kafka_config['sasl.username'].replace("${KAFKA_USERNAME}", os.getenv("KAFKA_USERNAME", "")).strip('"')
        kafka_config['sasl.password'] = kafka_config['sasl.password'].replace("${KAFKA_PASSWORD}", os.getenv("KAFKA_PASSWORD", "")).strip('"')
    
    if 'postgresql' in config_dict:
        postgresql_config = config_dict['postgresql']
        postgresql_config.setdefault('dbname', 'default_db')
        postgresql_config.setdefault('user', 'default_user')
        postgresql_config.setdefault('password', 'default_password')
        postgresql_config.setdefault('host', 'localhost')
        postgresql_config.setdefault('port', '5432')
    
    if 'topic' in config_dict:
        kafka_topic = config_dict['topic']
        kafka_topic['topic'] = kafka_topic.get('topic', '')
        
    return config_dict

config_dict = load_config()
kafka_config = config_dict['kafka']
postgresql_config = config_dict['postgresql']
topic = config_dict['topic']['topic']

# PostgreSQL connection setup
def get_postgresql_connection():
    conn = psycopg2.connect(
        dbname=postgresql_config['dbname'],
        user=postgresql_config['user'],
        password=postgresql_config['password'],
        host=postgresql_config['host'],
        port=postgresql_config['port']
    )
    return conn

# Function to store Kafka messages into PostgreSQL
def store_message_in_db(message, timestamp, kafka_offset):
    conn = get_postgresql_connection()
    cursor = conn.cursor()
    
    try:
        # Use ON CONFLICT to avoid duplicate messages based on the kafka_offset
        cursor.execute("""
            INSERT INTO kafka_data (message, timestamp, kafka_offset)
            VALUES (%s, %s, %s)
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

# Kafka Consumer setup
try:
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    logger.info("Kafka consumer subscribed to topic: %s", topic)

    # Consume Kafka messages and store them into PostgreSQL
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                logger.debug("No message received, waiting...")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition.")
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Process the message
            message_value = msg.value().decode('utf-8')
            kafka_offset = msg.offset()
            kafka_timestamp = msg.timestamp()[1]  # Get timestamp from Kafka metadata
            message_timestamp = datetime.fromtimestamp(kafka_timestamp / 1000.0)  # Convert to datetime

            logger.info(f"Processing message: Offset={kafka_offset}, Message={message_value}")

            try:
                store_message_in_db(message_value, message_timestamp, kafka_offset)  # Store message in PostgreSQL
                consumer.commit(asynchronous=False)  # Commit the offset after processing
                logger.info(f"Message committed successfully: Offset={kafka_offset}")
            except Exception as e:
                logger.error(f"Error storing message or committing offset: {e}")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")

finally:
    consumer.close()
    logger.info("Kafka consumer closed successfully.")
