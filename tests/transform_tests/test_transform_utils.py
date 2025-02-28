from moto import mock_aws
from utils.transform_utils.transform_lambda_utils import get_file
from utils.extraction_utils.lambda_utils import format_data_to_json, create_filename, upload_to_s3
import boto3
import pytest
import os
from datetime import datetime
from decimal import Decimal

@pytest.fixture(scope="function", autouse=False)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestGetFile:
    def test_get_file_returns_json(self):
        rows = [
            (1, 'Alice', datetime(2025, 2, 26, 14, 33), Decimal('100.00')),
            (2, 'Bob', datetime(2025, 2, 27, 15, 40), Decimal('200.50'))
        ]
        columns = ['id', 'name', 'timestamp', 'amount']
        with mock_aws():
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket='TestBucket', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            result = format_data_to_json(rows, columns)
            timestamp_for_filename = datetime.now().strftime("%Y/%m/%d/%H:%M")
            timestamp_for_last_extracted = timestamp_for_filename.encode("utf-8")
            s3_client.put_object(Bucket='TestBucket', Key=f'test_table/last_extracted.txt', Body=timestamp_for_last_extracted)
            filename = create_filename(table_name='test_table', time=timestamp_for_filename)
            upload_to_s3(data=result, bucket_name='TestBucket', object_name=filename)
            return_val = get_file(table='test_table', bucket_name='TestBucket')
            print(return_val)
            assert return_val == [{'id': 1, 'name': 'Alice', 'timestamp': '2025-02-26T14:33:00', 'amount': 100.0}, {'id': 2, 'name': 'Bob', 'timestamp': '2025-02-27T15:40:00', 'amount': 200.5}]
           



