from pg8000.native import Connection
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

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
