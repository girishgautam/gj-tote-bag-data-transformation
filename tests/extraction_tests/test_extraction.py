from utils.extraction_utils.lambda_utils import (
    collect_credentials_from_AWS,
    connection_to_database,
    check_for_data,
    upload_to_s3,
    create_filename,
    format_data_to_json,
)
from datetime import datetime
from unittest.mock import patch, MagicMock
from moto import mock_aws
from botocore.exceptions import ClientError
from decimal import Decimal
from src.extraction_lambda.main import extract_data
import boto3
import pytest
import os
import json


@pytest.fixture(scope="function", autouse=False)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestCollectCredentialsFromAWS:

    # The function obtains database access keys from AWS Secrets Manager
    def test_collect_credentials_returns_username_and_password(self):
        with mock_aws():
            sm_client = boto3.client("secretsmanager")
            secret_value = {"user_id": "test_user", "password": "test_password"}
            sm_client.create_secret(
                Name="Test_Name", SecretString=json.dumps(secret_value)
            )

            response = collect_credentials_from_AWS(sm_client, secret_id="Test_Name")

            assert response["user_id"] == "test_user"
            assert response["password"] == "test_password"


class TestConnectionToDatabase:

    # Use the collect credentials to obtain database AWS Secret

    def test_connection_to_database_can_run_query(self):
        db = connection_to_database()
        query = "SELECT * FROM payment LIMIT 2;"
        result = db.run(query)

        assert type(result) == list
        assert len(result) == 2


class TestCheckForData:

    def test_check_for_data_returns_false_if_bucket_empty(self):
        with mock_aws():
            # create mock s3 bucket
            s3_client = boto3.client("s3")
            # function checks for data in mock s3 bucket
            s3_client.create_bucket(
                Bucket="TestBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            # call check for data
            result = check_for_data(s3_client, bucket_name="TestBucket")

            assert result == False

    def test_check_for_data_returns_true_if_bucket_occupied(self):
        with mock_aws():
            # create mock s3 bucket
            s3_client = boto3.client("s3")
            # function checks for data in mock s3 bucket
            s3_client.create_bucket(
                Bucket="TestBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            # add data to mock s3 bucket
            s3_client.put_object(Body="Test.txt", Bucket="TestBucket", Key="TestKey")
            # call check for data
            result = check_for_data(s3_client, bucket_name="TestBucket")
            assert result == True


class TestUploadToS3:

    @mock_aws
    def test_upload_to_s3_success(self):
        """
        Tests that `upload_to_s3` successfully uploads data to an S3 bucket and verifies
        the uploaded content matches the original data.
        """
        bucket_name = "test-bucket"
        object_name = "test-object.txt"
        data = b"Sample data"

        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        upload_to_s3(data, bucket_name, object_name)

        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        assert response["Body"].read() == data

    @mock_aws
    def test_upload_to_s3_failure(self):
        """
        Verifies that `upload_to_s3` raises a `ClientError` when attempting to upload
        to a non-existent S3 bucket.
        """
        bucket_name = "nonexistent-bucket"
        object_name = "test-object.txt"
        data = b"Sample data"

        with pytest.raises(ClientError):
            upload_to_s3(data, bucket_name, object_name)


class TestCreateFilename:

    def test_create_filename(self):
        """
        Tests that `create_filename` generates the correct filename with a timestamp-based directory structure.
        """
        table_name = "test_table"
        mock_datetime = datetime(2025, 2, 25, 15, 9, 26, 123456)

        with patch("utils.extraction_utils.lambda_utils.datetime") as mock_dt:
            mock_dt.now.return_value = mock_datetime

            expected_filename = "test_table/2025/02/25/2025-02-25T15:09:26.123456.json"
            result = create_filename(table_name)
            assert result == expected_filename


class TestFormatDataToJson():

    def test_format_data_to_json(self):
        """
        Verify that format_data_to_json correctly converts rows and columns into
        a JSON-formatted bytes object, handling various data types such as datetime
        and Decimal.
        """
        rows = [
            (1, 'Alice', datetime(2025, 2, 26, 14, 33), Decimal('100.00')),
            (2, 'Bob', datetime(2025, 2, 27, 15, 40), Decimal('200.50'))
        ]
        columns = ['id', 'name', 'timestamp', 'amount']
        expected_data = [
            {'id': 1, 'name': 'Alice', 'timestamp': '2025-02-26T14:33:00', 'amount': 100.00},
            {'id': 2, 'name': 'Bob', 'timestamp': '2025-02-27T15:40:00', 'amount': 200.50}
        ]

        result = format_data_to_json(rows, columns)

        # Decode the result back to JSON object
        result_data = json.loads(result.decode('utf-8'))

        # Assert that the resulting JSON data matches the expected data
        assert result_data == expected_data

class TestExtractData():

    @patch('src.extraction_lambda.main.check_for_data')
    @patch('src.extraction_lambda.main.s3_client')
    @patch('src.extraction_lambda.main.conn')
    @patch('src.extraction_lambda.main.format_data_to_json')
    @patch('src.extraction_lambda.main.create_filename')
    @patch('src.extraction_lambda.main.upload_to_s3')
    def test_extract_data(self, mock_upload_to_s3, mock_create_filename, mock_format_data_to_json, mock_conn, mock_s3_client, mock_check_for_data):
        mock_conn.run.return_value = [{'id': 1, 'name': 'example'}]
        mock_conn.columns = [{'name': 'id'}, {'name': 'name'}]
        mock_check_for_data.return_value = True
        mock_s3_client.get_object.return_value = {'Body': MagicMock(read=lambda: b'2023-02-24T10:00:00')}
        mock_format_data_to_json.return_value = json.dumps([{'id': 1, 'name': 'example'}]).encode('utf-8')
        mock_create_filename.return_value = 'testfile.json'

        bucket_name = 'test-bucket'
        s3_client = mock_s3_client
        conn = mock_conn

        table_names = ['address', 'counterparty', 'design', 'sales_order',
                       'transaction', 'payment', 'payment_type',
                       'currency', 'staff', 'department', 'purchase_order']

        extraction_type, result_message = extract_data(s3_client, conn, bucket_name)

        assert extraction_type == 'Continuous extraction'
        assert 'Tables extracted' in result_message
        assert 'address' in result_message

        # Ensure that upload_to_s3 was called for each table
        assert mock_upload_to_s3.call_count == len(table_names)
        for table in table_names:
            mock_upload_to_s3.assert_any_call(mock_format_data_to_json.return_value, bucket_name, mock_create_filename.return_value)

        # Verify that the last_extracted timestamp is correctly formatted
        last_extracted = datetime.now().isoformat()
        actual_last_extracted_call = mock_s3_client.put_object.call_args[1]['Body'].decode('utf-8')
        assert actual_last_extracted_call.startswith(last_extracted[:19])

    @patch('src.extraction_lambda.main.check_for_data')
    @patch('src.extraction_lambda.main.s3_client')
    @patch('src.extraction_lambda.main.conn')
    @patch('src.extraction_lambda.main.format_data_to_json')
    @patch('src.extraction_lambda.main.create_filename')
    @patch('src.extraction_lambda.main.upload_to_s3')
    def test_initial_extraction(self, mock_upload_to_s3, mock_create_filename, mock_format_data_to_json, mock_conn, mock_s3_client, mock_check_for_data):
        mock_conn.run.return_value = [{'id': 1, 'name': 'example'}]
        mock_conn.columns = [{'name': 'id'}, {'name': 'name'}]
        mock_check_for_data.return_value = False  # No data available
        mock_format_data_to_json.return_value = json.dumps([{'id': 1, 'name': 'example'}]).encode('utf-8')
        mock_create_filename.return_value = 'testfile.json'

        bucket_name = 'test-bucket'
        s3_client = mock_s3_client
        conn = mock_conn

        table_names = ['address', 'counterparty', 'design', 'sales_order',
                       'transaction', 'payment', 'payment_type',
                       'currency', 'staff', 'department', 'purchase_order']

        extraction_type, result_message = extract_data(s3_client, conn, bucket_name)

        assert extraction_type == 'Initial extraction'
        assert 'Tables extracted' in result_message
        assert 'address' in result_message

        # Ensure that upload_to_s3 was called for each table
        assert mock_upload_to_s3.call_count == len(table_names)
        for table in table_names:
            mock_upload_to_s3.assert_any_call(mock_format_data_to_json.return_value, bucket_name, mock_create_filename.return_value)

        # Verify that the last_extracted timestamp is correctly formatted
        last_extracted = datetime.now().isoformat()
        actual_last_extracted_call = mock_s3_client.put_object.call_args[1]['Body'].decode('utf-8')
        assert actual_last_extracted_call.startswith(last_extracted[:19])  # Check up to seconds to avoid precision issues
