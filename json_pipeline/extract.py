import csv
import os
from dotenv import dotenv_values
from boto3 import client


def get_env_variables() -> dict:
    """
    Retrieves environment variables from the .env file.
    """
    try:
        return dotenv_values(".env")
    except Exception as e:
        raise Exception("Error retrieving variables from .env file") from e


def create_s3_client(access_key_id: str, secret_access_key: str) -> client:
    """
    Creates an S3 client using an AWS Access Key.
    """
    try:
        return client(
            "s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
    except Exception as e:
        raise Exception("Error creating S3 client") from e


def get_museum_bucket_files(s3_client: client, bucket_name: str) -> dict:
    """
    Retrieves the files from the bucket holding the museum visitor data,
    matching the desired prefix "lmnh".

    """
    try:
        return s3_client.list_objects_v2(Bucket=bucket_name, Prefix="lmnh")
    except Exception as e:
        raise Exception("Error retrieving files from the S3 bucket") from e


def is_relevant_csv_or_json(object_name: str) -> bool:
    """
    Returns True if the name of the object matches the specified and required format,
    otherwise returns False.
    """
    if (object_name.endswith(".csv") or object_name.endswith(".json")) and (
        object_name.startswith("lmnh_hist_data_")
        or object_name.startswith("lmnh_exhibition_")
    ):
        return True
    return False


def download_objects(
    objects: dict, s3_client: client, bucket_name: str, csv_path: str
) -> None:
    """
    Downloads all the relevant objects from the specified bucket, to the
    specified path, retaining the original name.
    """

    for obj in objects["Contents"]:
        object_name = obj["Key"]

        if is_relevant_csv_or_json(object_name):
            s3_client.download_file(
                bucket_name, object_name, f"{csv_path}/{object_name}"
            )


def get_all_csv_files(csv_directory: str) -> list[str]:
    """
    Returns a list of the names of all relevant CSV files in the specified path.
    """
    return [
        file
        for file in os.listdir(csv_directory)
        if file.endswith(".csv") and file.startswith("lmnh_hist_data_")
    ]


def combine_csv_files(csv_file_names: list[str], csv_path: str) -> None:
    """
    Creates a new csv and writes into it all the data from the passed in list of csv file names,
    which all have matching header rows.
    """

    merged_csv_filename = "merged_lmnh_hist_data.csv"

    merged_csv_file = open(
        f"{csv_path}/{merged_csv_filename}", "w", newline="", encoding="UTF-8"
    )

    fieldnames = None
    writer = csv.DictWriter(merged_csv_file, fieldnames=fieldnames)

    for file_name in csv_file_names:
        with open(f"{csv_path}/{file_name}", "r", encoding="UTF-8") as f:
            reader = csv.DictReader(f)

            if fieldnames == None:
                fieldnames = reader.fieldnames
                writer.fieldnames = fieldnames
                writer.writeheader()

            for row in reader:
                writer.writerow(row)

    merged_csv_file.close()


def remove_csv_files(csv_file_names: list[str], csv_path: str) -> None:
    """
    Deletes all files named in the passed in list.
    """
    for file_name in csv_file_names:
        if os.path.exists(f"{csv_path}/{file_name}"):
            os.remove(f"{csv_path}/{file_name}")
        else:
            print(f"{file_name} does not exist")


if __name__ == "__main__":
    pass
