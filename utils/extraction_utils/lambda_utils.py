from pg8000.native import Connection
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import json


def upload_to_s3(data, bucket_name, object_name):
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
        print(f'Successfully uploaded {object_name} to {bucket_name}')
    except ClientError as e:
        print(f'Failed to upload {object_name} to {bucket_name}: {e}')
        raise


def default_converter(row):
    if isinstance(row, datetime):
        return row.isoformat()

def create_filename(table_name):
    timestamp = datetime.now().isoformat()
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")

    filename = f"{table_name}/{year}/{month}/{day}/{timestamp}.json"
    return filename


def collect_credentials_from_AWS(sm_client, secret_id):

    response = sm_client.get_secret_value(SecretId=secret_id)
    response_json = json.loads(response["SecretString"])

    return response_json


def connection_to_database():

    sm_client = boto3.client('secretsmanager')
    secret_id = 'arn:aws:secretsmanager:eu-west-2:195275662632:secret:totesys_database-RBM0fV'

    response = collect_credentials_from_AWS(sm_client, secret_id)

    user = response['username']
    password = response['password']
    database = response['dbname']
    host = response['host']
    port = response['port']

    return Connection(user=user, password=password, database=database, host=host, port=port)


def check_for_data(s3_client, bucket_name):
    ''' This checks for presence of data in S3 Ingestion Bucket'''
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    print(response)
    return False if response['KeyCount'] < 1 else True


def format_data_to_json(rows, columns):
    data = [dict(zip(columns, row)) for row in rows]
    data_json = json.dumps(data, default=default_converter)
    return data_json

