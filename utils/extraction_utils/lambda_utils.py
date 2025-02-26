from pg8000.native import Connection
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import json


def upload_to_s3(data, bucket_name, object_name):
    """
    Uploads data to an S3 bucket.

    Args:
        data (str or bytes): The data to be uploaded to the S3 bucket.
        bucket_name (str): The name of the target S3 bucket.
        object_name (str): The name of the object to be created in the S3 bucket.

    Raises:
        ClientError: If the upload fails due to a client-side error with the AWS S3 service.

    Prints:
        A success message if the upload is successful, or an error message if the upload fails.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
        print(f"Successfully uploaded {object_name} to {bucket_name}")
    except ClientError as e:
        print(f"Failed to upload {object_name} to {bucket_name}: {e}")
        raise


def default_converter(row):
    """
    Converts a datetime object to an ISO 8601 formatted string.

    Args:
        row (Any): The value to be converted. If the value is a datetime object,
                   it will be converted to an ISO 8601 string.

    Returns:
        str or Any: An ISO 8601 formatted string if the input is a datetime object;
                    otherwise, returns the original input unchanged.
    """

    if isinstance(row, datetime):
        return row.isoformat()


def create_filename(table_name):
    """
    Generates a filename based on the current timestamp and the provided table name.

    Args:
        table_name (str): The name of the table to be included in the filename.

    Returns:
        str: A string representing the generated filename, formatted as
             "table_name/year/month/day/timestamp.json".
    """
    timestamp = datetime.now().isoformat()
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    filename = f"{table_name}/{year}/{month}/{day}/{timestamp}.json"
    return filename


def collect_credentials_from_AWS(sm_client, secret_id):
    """Returns credentials from AWS Secret Manager, function
    designed to be called from within connection_to_database function"""
    response = sm_client.get_secret_value(SecretId=secret_id)
    response_json = json.loads(response["SecretString"])

    return response_json


def connection_to_database():
    """Returns instance of pg8000 Connection for users to run database
    queries from"""

    sm_client = boto3.client("secretsmanager")
    secret_id = (
        "arn:aws:secretsmanager:eu-west-2:195275662632:secret:totesys_database-RBM0fV"
    )

    response = collect_credentials_from_AWS(sm_client, secret_id)

    user = response["username"]
    password = response["password"]
    database = response["dbname"]
    host = response["host"]
    port = response["port"]

    return Connection(
        user=user, password=password, database=database, host=host, port=port
    )


def check_for_data(s3_client, bucket_name):
    """This checks for presence of data in S3 Ingestion Bucket"""

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return False if response["KeyCount"] < 1 else True


def format_data_to_json(rows, columns):
    """
    Formats data from rows and columns into a JSON string.

    Args:
        rows (list of tuple): A list of rows, where each row is a tuple containing the data.
        columns (list of str): A list of column names corresponding to the data in the rows.

    Returns:
        str: A JSON-formatted string representing the data, where each row is converted
             to a dictionary with column names as keys.

    Notes:
        The function uses a custom converter for datetime objects, converting them into
        ISO 8601 formatted strings using the `default_converter` function.
    """
    data = [dict(zip(columns, row)) for row in rows]
    data_json = json.dumps(data, default=default_converter)
    return data_json

import io
def format_to_json(rows, columns):
    """Function receives rows and columns as arguments from either initial or continuous
    extract functions and creates a file-like object of JSON format in the buffer.
    The pointer in the buffer is reset to the beginning of the file and returns the buffer
    contents, so the file-like object can be put into S3 bucket with store_in_s3 function.
    Function allows to avoid potential security breaches that arise when data is saved locally.
    """
    data = [dict(zip(columns, row)) for row in rows]

    json_buffer = io.StringIO()
    json.dump(data, json_buffer)

    json_buffer.seek(0)

    return json_buffer
