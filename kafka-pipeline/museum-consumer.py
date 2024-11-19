from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import logging
import json


def setup_logger() -> logging.getLogger:
    """returns a configured logger."""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)


def load_message(response) -> dict:
    """Returns the consumed Message object as a Dictionary."""
    return json.loads(response.value().decode())


def sub_to_topic(kafka_consumer: Consumer, kafka_topic: str) -> None:
    try:
        kafka_consumer.subscribe([kafka_topic])
    except RuntimeError as re:
        raise RuntimeError("This consumer is closed!") from re


def main():

    # Create logger
    museum_logger = setup_logger()
    # Load environment variables
    load_dotenv()

    kafka_config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS"),
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
        "group.id": os.getenv("GROUP"),
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET"),
    }

    # Create Kafka consumer
    consumer = Consumer(kafka_config, logger=museum_logger)
    sub_to_topic(consumer, os.getenv("TOPIC_NAME"))

    # Keeps track of valid messages consumed
    message_count = 0
    try:

        while True:
            response = consumer.poll(timeout=1)
            if response is None:
                museum_logger.info("No event received")
            elif response:
                if response.error():
                    museum_logger.error(f"Kafka error: {response.error()}")

                if message_count % 20 == 0:
                    event = load_message(response)
                    museum_logger.info(event)
                message_count += 1

    except KeyboardInterrupt:
        museum_logger.info("Consumer stopped by user")
    except Exception as e:
        museum_logger.error({e})

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
