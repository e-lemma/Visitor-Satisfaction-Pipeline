import os
import logging
import json
from datetime import datetime, time
import argparse
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection, cursor


def setup_logger(log_to_file: bool) -> logging.getLogger:
    """returns a configured logger."""

    logger = logging.getLogger("lmnh")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s: %(message)s")

    if log_to_file is True:
        file_handler = logging.FileHandler("invalid_events.log", mode="a")
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def retrieve_args() -> argparse.Namespace:
    """Creates and runs an Argument Parser, then returns the extracted
    data in an argparse.Namespace object's attributes"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--logfile",
        action="store_true",
        help="Enables logging to file. By default, logging is to the console.",
    )
    return parser.parse_args()


def load_message(response) -> dict:
    """Returns the consumed Message object as a Dictionary."""
    return json.loads(response.value().decode())


def subscribe_to_topic(kafka_consumer: Consumer, kafka_topic: str) -> None:
    """Subscribe a Kafka consumer to a specified topic."""
    try:
        kafka_consumer.subscribe([kafka_topic])
    except RuntimeError as re:
        raise RuntimeError("This consumer is closed!") from re


def check_missing_values(message: dict) -> list[str]:
    """Checks if there are any missing values for each key in the message,
    if so, returns them in a list"""
    missing_values = [key for key, value in message.items() if value is None]
    return missing_values


def check_missing_keys(message: dict) -> list[str]:
    """Checks if there are any missing required keys in the message, if so,
    returns them in a list"""
    missing_keys = [key for key in ["at", "site", "val"] if key not in message]
    return missing_keys


def convert_iso_to_time(message: dict) -> time:
    """Converts an ISO 8601 time into a time object and returns it"""
    try:
        msg_datetime = datetime.fromisoformat(message["at"])
        return msg_datetime.time()
    except ValueError:
        raise ValueError(f"Invalid ISO 8601 format in 'at': {message['at']}")


def validate_message_time(
    message: dict, open_time: list[int], close_time: list[int]
) -> bool:
    """Checks that the time in the message is between the opening and closing times for
    the museum, returning true if so, and false if not."""
    msg_time = convert_iso_to_time(message)

    open_hour, open_minute = map(int, open_time)
    close_hour, close_minute = map(int, close_time)

    return time(open_hour, open_minute) <= msg_time <= time(close_hour, close_minute)


def validate_message_site(message: dict, valid_sites: list[str]) -> bool:
    """Returns true if the site in the message is valid, false if not"""
    if message["site"] not in valid_sites:
        return False
    return True


def validate_message_value(message: dict, valid_values: list[str]) -> bool:
    """Returns True if the value (rating) in the message is valid and
    an integer, false if not."""
    if not isinstance(message["val"], int) or str(message["val"]) not in valid_values:
        return False
    return True


def validate_message_type(message: dict, valid_types: list[str]) -> bool:
    """Returns true if the type is included in the message and is also valid"""

    if message["val"] == -1:
        if "type" not in message:
            return False
        if str(message["type"]) not in valid_types:
            return False
    elif "type" in message:
        return False
    return True


def identify_visitor_data_type(message: dict) -> bool:
    """
    Returns True if the data represents a visitor review, and false if an
    assistance request.
    """
    if message["val"] != -1:
        return True
    return False


def get_connection(config: dict) -> connection:
    """Creates a database session and returns a connection object"""
    return psycopg2.connect(
        database=config["database_name"],
        user=config["database_username"],
        host=config["database_ip"],
        password=config["database_password"],
        port=config["database_port"],
    )


def get_cursor(connection) -> cursor:
    """Creates a cursor to execute PostgreSQL commands"""
    return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


def insert_review_data(message: dict, connection: connection) -> None:
    """
    Inserts data from the event message into the psql review table, making sure
    the rating_id foreign key matches the primary key in the rating table.
    """

    with get_cursor(connection) as cur:

        exhibition_id = message["site"]
        rating_id = (message["val"]) + 1
        creation_date = message["at"]

        cur.execute(
            """
            INSERT INTO review(exhibition_id, rating_id, creation_date)
            VALUES (%s, %s, %s::TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC') 
            ON CONFLICT (exhibition_id, creation_date) DO NOTHING
            """,
            (exhibition_id, rating_id, creation_date),
        )

        connection.commit()


def insert_assistance_reqs_data(message: dict, connection: connection) -> None:
    """
    Inserts data from the event message into the psql assistance_request table,
    making sure the assistance_id foreign key matches the primary key in the assistance table.
    """
    with get_cursor(connection) as cur:

        exhibition_id = message["site"]
        assistance_id = (message["type"]) + 1
        creation_date = message["at"]

        cur.execute(
            """
            INSERT INTO assistance_request(exhibition_id, assistance_id, creation_date)
            VALUES (%s, %s, %s::TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC')
            ON CONFLICT (exhibition_id, creation_date) DO NOTHING
            """,
            (exhibition_id, assistance_id, creation_date),
        )

        connection.commit()


def main():

    # Load environment variables
    load_dotenv()
    # Get CLI arguments from argparse
    args = retrieve_args()
    # Create logger
    museum_logger = setup_logger(args.logfile)

    # Loads the configuration details for kafka, database connections
    kafka_config = {
        "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": os.getenv("SECURITY_PROTOCOL"),
        "sasl.mechanisms": os.getenv("SASL_MECHANISMS"),
        "sasl.username": os.getenv("SASL_USERNAME"),
        "sasl.password": os.getenv("SASL_PASSWORD"),
        "group.id": os.getenv("GROUP"),
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET"),
    }
    database_connection_config = {
        "database_name": os.getenv("DATABASE_NAME"),
        "database_username": os.getenv("DATABASE_USERNAME"),
        "database_ip": os.getenv("DATABASE_IP"),
        "database_password": os.getenv("DATABASE_PASSWORD"),
        "database_port": os.getenv("DATABASE_PORT"),
    }

    # Loads several necessary variables
    valid_sites = os.getenv("VALID_SITES").split(",")
    valid_values = os.getenv("VALID_VALUES").split(",")
    valid_types = os.getenv("VALID_TYPES").split(",")
    open_time = os.getenv("OPEN_TIME").split(",")
    close_time = os.getenv("CLOSE_TIME").split(",")

    # Create Kafka consumer
    consumer = Consumer(kafka_config, logger=museum_logger)
    subscribe_to_topic(consumer, os.getenv("TOPIC_NAME"))

    consumer_count = 0

    try:

        while consumer_count < 100000:
            response = consumer.poll(timeout=1)

            if response is None:
                museum_logger.info("No event received")
                continue
            if response.error():
                museum_logger.error("Kafka error: %s", response.error())
                continue

            event = load_message(response)

            missing_values = check_missing_values(event)
            missing_keys = check_missing_keys(event)

            if missing_values:
                museum_logger.error(
                    "INVALID: %s (values for %s missing)", event, missing_values
                )
                continue
            if missing_keys:
                museum_logger.error(
                    "INVALID: %s (%s key/s missing)", event, missing_keys
                )
                continue

            elif not validate_message_time(event, open_time, close_time):
                museum_logger.error(
                    "INVALID: %s (Time is not within Museum's open hours)", event
                )

            elif not validate_message_site(event, valid_sites):
                museum_logger.error("INVALID: %s (site is invalid!)", event)

            elif not validate_message_value(event, valid_values):
                museum_logger.error("INVALID: %s (value is invalid!)", event)
            elif not validate_message_type(event, valid_types):
                museum_logger.error("INVALID: %s (type is invalid!)", event)

            else:
                museum_logger.info(event)

                conn = get_connection(database_connection_config)
                if identify_visitor_data_type(event):
                    insert_review_data(event, conn)
                else:
                    insert_assistance_reqs_data(event, conn)
                conn.close()

            consumer_count += 1

            if consumer_count >= 100000:
                museum_logger.info(
                    "Reached the consumer limit! Stopping the consumer..."
                )

    except KeyboardInterrupt:
        museum_logger.info("Consumer stopped by user")
    except KafkaError:
        museum_logger.error("Kafka error: %s", KafkaError)

    finally:
        consumer.close()
        museum_logger.info("Kafka consumer closed successfully.")


if __name__ == "__main__":
    main()
