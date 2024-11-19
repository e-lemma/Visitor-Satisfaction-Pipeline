# Kafka Pipeline

## Overview

This pipeline automates the extraction, processing, and loading of data from a Kafka topic into a PostgreSQL database. It consumes messages from a Kafka topic, processes the data, and stores it in a PostgreSQL database.

## Installation

### Prerequisites

- Python 3.12+
- Kafka broker
- PostgreSQL database

### Steps

1. **Install dependencies**:
    ```bash
    pip3 install -r requirements.txt
    ```

2. **Set up environment variables**:
    Create a `.env` file with the following format:
    ```env
    # Kafka Credentials
    BOOTSTRAP_SERVERS=your_kafka_broker
    SECURITY_PROTOCOL=your_security_protocol
    SASL_MECHANISMS=your_sasl_mechanisms
    SASL_USERNAME=your_kafka_username
    SASL_PASSWORD=your_kafka_password
    GROUP=your_kafka_group
    AUTO_OFFSET_RESET=your_auto_offset_reset

    # Database Credentials
    DATABASE_NAME=your_db_name
    DATABASE_USERNAME=your_db_username
    DATABASE_IP=your_db_host
    DATABASE_PASSWORD=your_db_password
    DATABASE_PORT=your_db_port

    # Valid Values
    VALID_SITES=0,1,2,3,4,5
    VALID_VALUES=-1,0,1,2,3,4
    VALID_TYPES=0,1

    # Operating Hours
    OPEN_TIME=8,45
    CLOSE_TIME=18,15

    # Kafka Topic
    TOPIC_NAME=your_kafka_topic
    ```

3. **Ensure Kafka broker access** to the specified topic.

## Running the Pipeline

1. **Run the pipeline script**:
    ```bash
    python3 lmnh_pipeline.py
    ```
    To enable logging to a file (by default, it logs to the console), use the `--logfile` flag:
    ```bash
    python3 lmnh_pipeline.py --logfile
    ```
    This will consume messages from the Kafka topic, process the data, and insert it into the PostgreSQL database.

## Configuration

- Kafka broker, topic, and database credentials should be set in the `.env` file.

## Testing

- **Run unit tests**:
    ```bash
    python3 -m unittest test_lmnh_pipeline.py
    ```

## Additional Scripts

- **Clear Database**:
    Run the `clear_database.sql` script to clear the database tables:
    ```bash
    psql -h your_db_host -U your_db_username -d your_db_name -f clear_database.sql
    ```

- **Museum Consumer**:
    The `museum-consumer.py` script can be used to consume messages from the Kafka topic:
    ```bash
    python3 museum-consumer.py
    ```

## Troubleshooting

- **Missing dependencies**: Run `pip install -r requirements.txt`
- **Invalid credentials**: Check the `.env` file for any errors.
- **Kafka connection issues**: Ensure the Kafka broker is running and accessible.
