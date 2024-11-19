# JSON Pipeline

## Overview

This pipeline automates the extraction, merging, and loading of museum visitor data from an AWS S3 bucket into a PostgreSQL database. It takes data in JSON format, processes it into reviews and assistance requests, and stores them in separate database tables.

## Installation

### Prerequisites

- Python 3.12+
- AWS credentials (Access Key ID, Secret Access Key)
- PostgreSQL database

### Steps

1. **Install dependencies**:
    ```bash
    pip3 install -r requirements.txt
    ```

2. **Set up environment variables**:
    Create a `.env` file with the following format:
    ```env
    ACCESS_KEY=your_aws_access_key
    SECRET_ACCESS_KEY=your_aws_secret_key
    DATABASE_NAME=your_db_name
    DATABASE_USERNAME=your_db_username
    DATABASE_IP=your_db_host
    DATABASE_PASSWORD=your_db_password
    DATABASE_PORT=your_db_port
    BUCKET=your_s3_bucket_name
    CSV_PATH=your_local_csv_path
    ```

3. **Ensure S3 access** to the bucket with museum data.

## Running the Pipeline

1. **Run the pipeline script**:
    ```bash
    python3 pipeline.py
    ```
    To enable logging to a file (by default, it logs to the console), use the `--logfile` flag:
    ```bash
    python3 pipeline.py --logfile
    ```
    This will automatically retrieve, merge, and clean the data from the S3 bucket and insert it into the PostgreSQL database.

## Configuration

- AWS and database credentials should be set in the `.env` file.
- The CSV path specifies where the merged file will be saved locally.

## Troubleshooting

- **Missing dependencies**: Run `pip install -r requirements.txt`
- **Invalid credentials**: Check the `.env` file for any errors.


