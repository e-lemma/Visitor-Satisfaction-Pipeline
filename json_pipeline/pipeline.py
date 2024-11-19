import extract
import argparse
import logging
import csv
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection, cursor


def init_logger(log_to_file: bool) -> logging.Logger:
    """Sets up a logger"""

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    if log_to_file is True:
        file_handler = logging.FileHandler("pipeline.log", mode="w")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-b",
        "--bucket",
        help="Specify the name of the bucket to retrieve visitor data from.",
    )
    parser.add_argument(
        "-n",
        "--numrows",
        type=int,
        help="Specify the number of rows to load to the database",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        action="store_true",
        help="Enables logging to file, default is logging to console.",
    )
    return parser.parse_args()


def get_connection() -> connection:
    """Creates a database session and returns a connection object"""
    return psycopg2.connect(
        database=database_name,
        user=database_username,
        host=database_ip,
        password=database_password,
        port=database_port,
    )


def get_cursor(connection) -> cursor:
    """Creates a cursor to execute PostgreSQL commands"""
    return connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


def load_csv(filename: str, num_rows: int = None) -> list[dict]:
    """Opens a .csv and returns the data as a list[dict]"""
    data = []
    with open(filename, encoding="utf-8") as f:
        for i, line in enumerate(csv.DictReader(f)):
            if num_rows and i > num_rows:
                break
            data.append(line)
    return data


def split_reviews_and_assistance_reqs(
    data: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Separates the two different visitor 'button press' data types, reviews and
    assistance requests, and returns them as two separate lists in a tuple.
    """
    reviews = [row for row in data if (row["val"] != "-1")]
    assistance_requests = [row for row in data if (row["val"] == "-1")]
    return (reviews, assistance_requests)


def insert_review_data(review_data: list[dict], connection: connection) -> None:
    """
    Inserts data from the review_data list into the psql review table, making sure
    the rating_id foreign key matches the primary key in the rating table.
    """

    with get_cursor(connection) as cursor:
        logger.info("Inserting review data...")
        for row in review_data:

            exhibition_id = row["site"]
            rating_id = int(row["val"]) + 1
            creation_date = row["at"]

            cursor.execute(
                """INSERT INTO review(exhibition_id, rating_id, creation_date) VALUES (%s, %s, %s) 
                ON CONFLICT (exhibition_id, creation_date) DO NOTHING""",
                (exhibition_id, rating_id, creation_date),
            )

        logger.info("Insertion Complete")
        connection.commit()


def insert_assistance_reqs_data(
    assistance_request_data: list[dict], connection: connection
) -> None:
    """
    Inserts data from the assistance_request_data list into the psql assistance_request table,
    making sure the assistance_id foreign key matches the primary key in the assistance table.
    """
    with get_cursor(connection) as cursor:
        logger.info("Inserting assistance request data...")
        for row in assistance_request_data:

            exhibition_id = row["site"]
            assistance_id = int(row["type"][0]) + 1
            creation_date = row["at"]

            cursor.execute(
                """INSERT INTO assistance_request(exhibition_id, assistance_id, creation_date) VALUES (%s, %s, %s) 
                ON CONFLICT (exhibition_id, creation_date) DO NOTHING""",
                (exhibition_id, assistance_id, creation_date),
            )

        logger.info("Insertion Complete")
        connection.commit()


if __name__ == "__main__":

    args = parse_args()

    logger = init_logger(args.logfile)

    # Retrieve AWS credentials and Database connection information
    env_var = extract.get_env_variables()

    access_key_id = env_var.get("ACCESS_KEY")
    secret_access_key = env_var.get("SECRET_ACCESS_KEY")

    database_name = env_var.get("DATABASE_NAME")
    database_username = env_var.get("DATABASE_USERNAME")
    database_ip = env_var.get("DATABASE_IP")
    database_password = env_var.get("DATABASE_PASSWORD")
    database_port = env_var.get("DATABASE_PORT")

    # Use bucket name from .env if command line argument not present
    if args.bucket:
        museum_bucket_name = args.bucket
    else:
        museum_bucket_name = env_var.get("BUCKET")
    csv_path = env_var.get("CSV_PATH")

    logger.info("All environment variables loaded!")

    # Access AWS S3 bucket and download relevant files
    s3 = extract.create_s3_client(access_key_id, secret_access_key)
    museum_objects = extract.get_museum_bucket_files(s3, museum_bucket_name)
    extract.download_objects(museum_objects, s3, museum_bucket_name, csv_path)

    logger.info("All files downloaded from S3 bucket!")

    # Merge csv data into one file and delete redundant files
    relevant_csv_filenames = extract.get_all_csv_files(csv_path)
    extract.combine_csv_files(relevant_csv_filenames, csv_path)
    extract.remove_csv_files(relevant_csv_filenames, csv_path)

    logger.info("CSV files successfully merged and redundant files removed!")

    # Load the data and split it into two lists
    visitor_data = load_csv("pipeline/merged_lmnh_hist_data.csv", args.numrows)
    review_data, assistance_request_data = split_reviews_and_assistance_reqs(
        visitor_data
    )

    logger.info("Data successfully cleaned and separated!")

    # Insert the data into the two tables
    try:
        conn = get_connection()
    except psycopg2.Error as e:
        logger.error(f"Failed to establish database connection: {e}")
        raise
    insert_review_data(review_data, conn)
    logger.info("All review data inserted successfully!")
    insert_assistance_reqs_data(assistance_request_data, conn)
    logger.info("All assistance request data inserted successfully!")
    conn.close()
