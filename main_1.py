from kafka import consume_message, produce_message
from db import store_message_in_db
from config import logger
import threading


def forward_to_local(message, timestamp, kafka_offset):
    """Forward messages from Kafka Server to Kafka Local."""
    logger.info("Forwarding message to Kafka Local...")
    produce_message("kafka_local", "kafka_local_topic", message)


def save_to_db(message, timestamp, kafka_offset):
    """Save messages from Kafka Local to the database."""
    logger.info("Saving message to database...")
    store_message_in_db(message, timestamp, kafka_offset)

def main():
    # Step 1: Consume from Kafka Server and forward to Kafka Local
    local_producer_handle = consume_message("kafka_server", ["product_view"], forward_to_local)
    
    # Step 2: Consume from Kafka Local and store in database
    postgres_handler = consume_message("kafka_local", ["product_view"], save_to_db)

    # Create threads for each handler
    threading1 = threading.Thread(target=local_producer_handle.run)  # Pass method reference
    threading2 = threading.Thread(target=postgres_handler.run)

    # Start threads
    threading1.start()
    threading2.start()

    # Wait for threads to finish
    threading1.join()
    threading2.join()


if __name__ == "__main__":
    main()
