from pg8000.native import Connection
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import json


def upload_to_s3(data, bucket_name, object_name):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
        print(f'Successfully uploaded {object_name} to {bucket_name}')
    except ClientError as e:
        print(f'Failed to upload {object_name} to {bucket_name}: {e}')


def default_converter(row):
    if isinstance(row, datetime):
        return row.isoformat()

def create_filename(table_name):
    timestamp = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
    # year = timestamp.year
    # month = timestamp.month
    # day = timestamp.day
    # hour = timestamp.hour
    # minute = timestamp.minute
    # seconds = timestamp.second
    print(timestamp)

    # filename = f"{table_name}/{year}/{month}/{day}/{hour}/{minute}/{timestamp}.json"
    # return filename

table_name = "example_table"
filename = create_filename(table_name)
print(filename)


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

