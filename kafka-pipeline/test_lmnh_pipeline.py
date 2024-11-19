from unittest.mock import MagicMock, patch
import os
from lmnh_pipeline import (
    convert_iso_to_time,
    validate_message_time,
    validate_message_site,
    validate_message_value,
    validate_message_type,
    identify_visitor_data_type,
    insert_review_data,
    get_cursor,
    insert_assistance_reqs_data,
)


""" Testing that ISO 8601 time is properly converted to a time object """


def test_convert_iso_to_time():

    test_valid_message = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "4",
        "val": 1,
    }
    expected = "12:54:44.952732"

    assert str(convert_iso_to_time(test_valid_message)) == expected


"""""" """""" """""" """""" """""" """""" """"""
""" Testing Data validation functions """
"""""" """""" """""" """""" """""" """""" """"""

test_valid_message = {"at": "2024-05-06T12:54:44.952732+01:00", "site": "4", "val": 1}
valid_sites = os.getenv("VALID_SITES").split(",")
valid_values = os.getenv("VALID_VALUES").split(",")
valid_types = os.getenv("VALID_TYPES").split(",")
open_time = os.getenv("OPEN_TIME").split(",")
close_time = os.getenv("CLOSE_TIME").split(",")

""" Testing message time validation """


def test_validate_message_time_valid():
    assert validate_message_time(test_valid_message, open_time, close_time) == True


def test_validate_message_time_invalid():

    test_invalid_message = {
        "at": "2024-05-06T19:54:44.952732+01:00",
        "site": "4",
        "val": 1,
    }
    assert validate_message_time(test_invalid_message, open_time, close_time) == False


""" Testing message exhibition site validation """


def test_validate_message_site_valid():
    assert validate_message_site(test_valid_message, valid_sites) == True


def test_validate_message_site_invalid():
    test_invalid_message = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "21",
        "val": 1,
    }
    assert validate_message_site(test_invalid_message, valid_sites) == False


def test_validate_message_site_invalid_site_type():
    test_invalid_message = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": 2,
        "val": 1,
    }
    assert validate_message_site(test_invalid_message, valid_sites) == False


""" Testing message rating/value validation """


def test_validate_message_value_valid():
    assert validate_message_value(test_valid_message, valid_values) == True


def test_validate_message_value_invalid():
    test_invalid_message = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": 11,
    }

    assert validate_message_value(test_invalid_message, valid_values) == False


def test_validate_message_value_invalid_value_type():
    test_invalid_message = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": "0",
    }

    assert validate_message_value(test_invalid_message, valid_values) == False


""" Testing message type validation """


def test_validate_message_type_valid_without_type():
    assert validate_message_type(test_valid_message, valid_types) == True


def test_validate_message_type_valid_with_type():
    test_valid_message_type = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": -1,
        "type": 0,
    }
    assert validate_message_type(test_valid_message_type, valid_types) == True


def test_validate_message_type_invalid_value():
    test_invalid_message_type = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": -1,
        "type": 6,
    }
    assert validate_message_type(test_invalid_message_type, valid_types) == False


def test_validate_message_type_missing_type():
    test_invalid_message_type = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": -1,
    }
    assert validate_message_type(test_invalid_message_type, valid_types) == False


""" Testing correct visitor data type identified """


def test_identify_visitor_data_type_review():
    assert identify_visitor_data_type(test_valid_message) == True


def test_identify_visitor_data_type_assistance_request():
    test_valid_message_type = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": -1,
        "type": 0,
    }
    assert identify_visitor_data_type(test_valid_message_type) == False


"""""" """""" """""" """""" """""" """"""
""" Testing Database Insertion """
"""""" """""" """""" """""" """""" """"""


def test_insert_review_data():

    test_valid_message = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "4",
        "val": 1,
    }

    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    insert_review_data(test_valid_message, mock_connection)

    mock_cursor.execute.assert_called_once_with(
        """
            INSERT INTO review(exhibition_id, rating_id, creation_date)
            VALUES (%s, %s, %s::TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC') 
            ON CONFLICT (exhibition_id, creation_date) DO NOTHING
            """,
        (
            test_valid_message["site"],
            test_valid_message["val"] + 1,
            test_valid_message["at"],
        ),
    )

    mock_connection.commit.assert_called_once()


def test_insert_assistance_reqs_data():

    test_valid_message_type = {
        "at": "2024-05-06T12:54:44.952732+01:00",
        "site": "2",
        "val": -1,
        "type": 0,
    }

    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    insert_assistance_reqs_data(test_valid_message_type, mock_connection)

    mock_cursor.execute.assert_called_once_with(
        """
            INSERT INTO assistance_request(exhibition_id, assistance_id, creation_date)
            VALUES (%s, %s, %s::TIMESTAMP WITH TIME ZONE AT TIME ZONE 'UTC')
            ON CONFLICT (exhibition_id, creation_date) DO NOTHING
            """,
        (
            test_valid_message_type["site"],
            test_valid_message_type["type"] + 1,
            test_valid_message_type["at"],
        ),
    )

    mock_connection.commit.assert_called_once()
