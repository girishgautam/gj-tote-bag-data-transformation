from utils.extraction_utils.lambda_utils import (
    collect_credentials_from_AWS,
    connection_to_database,
    check_for_data,
    upload_to_s3,
    default_converter,
    create_filename,
    format_data_to_json,
)
from datetime import datetime, timezone
from unittest.mock import patch
from moto import mock_aws
from botocore.exceptions import ClientError
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


class TestDefaultConverter:

    def test_default_converter_with_datetime(self):
        """
        Tests that `default_converter` correctly converts a datetime object to an ISO 8601 string.
        """
        dt = datetime(2023, 3, 14, 15, 9, 26)
        assert default_converter(dt) == "2023-03-14T15:09:26"


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


class TestFormatDataToJson:

    @pytest.fixture
    def sample_data(self):
        rows = [(1, "Alice", 25), (2, "Bob", 30)]
        columns = ["id", "name", "age"]
        return rows, columns

    def test_format_data_to_json(self, sample_data):
        """
        Tests that `format_data_to_json` correctly converts rows and columns into a JSON string.
        """
        rows, columns = sample_data
        expected_output = json.dumps(
            [{"id": 1, "name": "Alice", "age": 25}, {"id": 2, "name": "Bob", "age": 30}]
        )

        assert format_data_to_json(rows, columns) == expected_output
