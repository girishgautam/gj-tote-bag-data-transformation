from pg8000.native import Connection
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from decimal import Decimal
import json
import io


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
        # print(f"Successfully uploaded {object_name} to {bucket_name}")
    except ClientError as e:
        # print(f"Failed to upload {object_name} to {bucket_name}: {e}")
        raise


def create_filename(table_name, time):
    """
    Generates a filename based on the current timestamp and the provided table name.

    Args:
        table_name (str): The name of the table to be included in the filename.

    Returns:
        str: A string representing the generated filename, formatted as
             "table_name/year/month/day/timestamp.json".
    """
    # timestamp = datetime.now().isoformat()
    # year = datetime.now().strftime("%Y")
    # month = datetime.now().strftime("%m")
    # day = datetime.now().strftime("%d")

    filename = f"{table_name}/{time}.json"
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


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
            # return obj.strftime("%Y/%m/%d/%H:%M")
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def format_data_to_json(rows, columns):
    """
    Convert data from rows and columns into a JSON-formatted bytes object.

    Args:
        rows (list of tuple): A list of tuples containing the data to be converted.
        columns (list of str): A list of column names corresponding to the data in rows.

    Returns:
        bytes: A bytes object containing the JSON-formatted data.

    The function uses a custom JSON encoder (CustomEncoder) to handle non-serializable
    objects such as datetime and Decimal. The data is first converted into a list of
    dictionaries, where each dictionary represents a row of data with column names as keys.
    The resulting list of dictionaries is then serialized into JSON and returned as a
    UTF-8 encoded bytes object.
    """
    data = [dict(zip(columns, row)) for row in rows]

    json_buffer = io.StringIO()
    json.dump(data, json_buffer, cls=CustomEncoder)

    json_buffer.seek(0)

    return json_buffer.getvalue().encode("utf-8")


def get_s3_bucket_name(bucket_prefix):
    """
    Retrieve the name of the  S3 bucket that starts with the specified prefix.
    ingest_bucket_prefix - "data-squid-ingest-bucket-"
    transform_bucket_prefix - "data-squid-transform-bucket-"

    Parameters:
    bucket_prefix (str): The prefix to match against the names of S3 buckets.

    Returns:
    str: The name of the first S3 bucket that starts with the given prefix.


    """

    s3_client = boto3.client("s3")

    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        if bucket["Name"].startswith(bucket_prefix):
            return bucket["Name"]
    else:
        raise ValueError("Error: bucket prefix not found")
