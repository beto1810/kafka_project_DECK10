#import Library
import configparser
from confluent_kafka import Producer,Consumer, KafkaException, KafkaError
import psycopg2
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import re
import json


# Load environmental variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# Parsing info from config file
config = configparser.ConfigParser()
config.read("kafka_config.ini")


def parse_env_variable(value):
    """Parse environment variable with default value fallback."""
    match = re.match(r'\$\{([^:}]+):?([^}]*)\}', value)
    if match:
        env_var, default_value = match.groups()
        return os.getenv(env_var, default_value)
    return value

def load_config(section_name):
    """Load and return config section with environment variable substitutions."""
    if section_name not in config:
        raise ValueError(f"Section '{section_name}' not found in the configuration file.")
    
    # Replace environment variables in the section
    section_config = {key: parse_env_variable(value) for key, value in config.items(section_name)}
    return section_config


# try:
#     kafka_config = load_config("kafka")
#     logger.debug("Kafka configuration loaded successfully.")
    
#     # Print only the Kafka section
#     print("Kafka Configuration:")
#     print(kafka_config)
    
#     # Initialize Kafka consumer
#     consumer = Consumer(kafka_config)
#     consumer.subscribe(["product_view"])
#     logger.info("Kafka Consumer initialized successfully.")

# except ValueError as e:
#     logger.error(f"Error loading configuration: {e}")
# except KafkaException as e:
#     logger.error(f"Kafka error: {e}")
# except Exception as e:
#     logger.error(f"Unexpected error: {e}")


# try:
#     kafka_local_config = load_config("kafka_local")
#     logger.debug("Kafka configuration loaded successfully.")
    
#     # Print only the Kafka section
#     print("Kafka Configuration:")
#     print(kafka_local_config)
    
#     # Initialize Kafka Producer
#     producer = Producer(kafka_local_config)
#     logger.info("Kafka Producer initialized successfully.")

# except ValueError as e:
#     logger.error(f"Error loading configuration: {e}")
# except KafkaException as e:
#     logger.error(f"Kafka error: {e}")
# except Exception as e:
#     logger.error(f"Unexpected error: {e}")


# # Hàm callback khi gửi dữ liệu thành công
    # def delivery_report(err, msg):
    #     if err is not None:
    #         logger.error(f"Message delivery failed: {err}")
    #     else:
    #         logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# # Đọc và gửi lại dữ liệu vào topic 'destination_topic'
# try:
#     while True:
#         msg = consumer.poll(timeout=1.0)  # Chờ 1 giây để nhận dữ liệu từ Kafka

#         if msg is None:  # Không có message
#             continue
#         if msg.error():  # Kiểm tra lỗi
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 logger.info(f'End of partition reached {msg.partition()}/{msg.offset()}')
#             else:
#                 logger.error(f"Consumer error: {msg.error()}")
#         else:
#             # Lấy dữ liệu từ message
#             data = msg.value().decode('utf-8')
#             data_json = json.loads(data)

#             # Có thể xử lý dữ liệu tại đây (ví dụ: thay đổi dữ liệu)
#             # data_json['processed'] = process_data(data_json)

#             # Gửi dữ liệu vào topic 'test_3'
#             producer.produce( "product_view", value=json.dumps(data_json), callback=delivery_report)

#             # Đảm bảo đã gửi dữ liệu
#             producer.flush()

# except KeyboardInterrupt:
#     pass
# finally:
#     # Đóng consumer và producer khi xong
#     consumer.close()
#     producer.close()
