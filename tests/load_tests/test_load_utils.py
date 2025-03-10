from utils.lambda_utils import parquet_to_dataframe
import boto3
from moto import mock_aws
import os
import pytest
import pandas as pd
import json
from unittest.mock import patch, MagicMock, Mock
from utils.lambda_utils import connect_to_warehouse, insert_data_to_table
from src.load_lambda.main import lambda_handler


class TestParquetToDataframe:

    def test_returns_dataframe(self):
        table = "addresses"
        last_extract_time = "2025/03/06/13:39"
        with mock_aws():
            test_bucket = "TestBucket"
            s3_client = boto3.client("s3")
            s3_client.create_bucket(
                Bucket=test_bucket,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            df = pd.DataFrame.from_dict({"column1": ["value1", "value2"]})
            parquet_byte_stream = df.to_parquet()
            s3_client.put_object(
                Body=parquet_byte_stream,
                Bucket=test_bucket,
                Key=f"{table}/{last_extract_time}.pqt",
            )

            last_transformed_filename = "last_transformed.txt"
            with open(last_transformed_filename, mode="w") as f:
                f.write(last_extract_time)
            with open(last_transformed_filename, mode="r") as f:
                s3_client.put_object(
                    Body=f.buffer,
                    Bucket=test_bucket,
                    Key=f"{table}/{last_transformed_filename}",
                )
            os.remove(last_transformed_filename)

            expected_output = parquet_to_dataframe(s3_client, test_bucket, table)
            expected_result = pd.DataFrame.from_dict({"column1": ["value1", "value2"]})

            assert isinstance(expected_result, pd.DataFrame)
            assert expected_output.to_string() == expected_result.to_string()


class TestConnectToWarehouse:

    @patch("utils.lambda_utils.boto3.client")
    @patch("utils.lambda_utils.pg8000.connect")
    def test_connect_to_warehouse(self, mock_pg8000_connect, mock_boto3_client):
        """
        Tests the connect_to_warehouse function by mocking Secrets Manager client
        and pg8000.connect.

        Args:
            self: The instance of the test case.
            mock_pg8000_connect (MagicMock): Mocked pg8000.connect method.
            mock_boto3_client (MagicMock): Mocked boto3 client method.

        """
        mock_sm_client = MagicMock()
        mock_boto3_client.return_value = mock_sm_client

        # Mock the response from collect_credentials_from_AWS
        mock_sm_client.get_secret_value.return_value = {
            "SecretString": '{"username": "test_user", "password": "test_pass", "dbname": "test_db", "host": "test_host", "port": 5432}'
        }
        with patch(
            "utils.lambda_utils.collect_credentials_from_AWS",
            return_value={
                "username": "test_user",
                "password": "test_pass",
                "dbname": "test_db",
                "host": "test_host",
                "port": 5432,
            },
        ):
            conn = connect_to_warehouse()

            mock_pg8000_connect.assert_called_once_with(
                user="test_user",
                database="test_db",
                password="test_pass",
                host="test_host",
                port=5432,
            )

            assert conn == mock_pg8000_connect.return_value


class TestInsertDataToTable:

    @pytest.fixture
    def mock_conn(self):
        """Fixture to create a mock database connection and cursor."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn

    def test_insert_data_to_table(self, mock_conn):
        """Test inserting data into the table with mocked database connection."""

        # Mock DataFrame
        df = pd.DataFrame(
            {
                "sales_record_id": [1, 2],
                "column1": ["value1", "value2"],
                "column2": ["value3", "value4"],
            }
        )

        table_name = "test_table"

        insert_data_to_table(mock_conn, table_name, df)

        # Assert cursor methods are called
        mock_conn.cursor.assert_called_once()
        mock_cursor = mock_conn.cursor.return_value

        # Check that execute() was called for each row in df
        assert mock_cursor.execute.call_count == len(df)

        # Verify the SQL query format
        expected_query = f"""
            INSERT INTO {table_name} (sales_record_id, column1, column2)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        expected_calls = [
            ((expected_query, tuple(row)),)
            for row in df.itertuples(index=False, name=None)
        ]

        mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)

        # Ensure commit() and cursor.close() are called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestLoadLambdaHandler:

    @pytest.fixture
    def mock_event(self):
        """Fixture to provide a mock event for S3 triggers."""
        return {
            "Records": [
                {
                    "s3": {
                        "object": {"key": "test_file.parquet"},
                        "bucket": {"name": "test_bucket"},
                    }
                }
            ]
        }

    @patch("src.load_lambda.main.logger")
    @patch("src.load_lambda.main.extract_tablenames_load")
    @patch("src.load_lambda.main.connect_to_warehouse")
    @patch("src.load_lambda.main.parquet_to_dataframe")
    @patch("src.load_lambda.main.insert_data_to_table")
    def test_lambda_handler_success(
        self,
        mock_insert_data_to_table,
        mock_parquet_to_dataframe,
        mock_connect_to_warehouse,
        mock_extract_tablenames_load,
        mock_logger,
        mock_event,
    ):
        # Mock dependencies
        mock_extract_tablenames_load.return_value = ["dim_date", "dim_staff"]
        mock_connect_to_warehouse.return_value = Mock()
        mock_parquet_to_dataframe.return_value = Mock()

        # Invoke the lambda handler
        context = {}
        response = lambda_handler(mock_event, context)

        # Assert logger was called
        mock_logger.info.assert_called()
        mock_logger.info.assert_any_call(
            "Processing file %s from bucket %s", "test_file.parquet", "test_bucket"
        )

        # Assert the mock functions were called as expected
        mock_extract_tablenames_load.assert_called_once_with(
            "test_bucket", "test_file.parquet"
        )
        mock_connect_to_warehouse.assert_called_once()
        assert mock_parquet_to_dataframe.call_count == 2  # Called for each valid table
        assert mock_insert_data_to_table.call_count == 2  # Called for each valid table

        # Assert the response
        assert response["statusCode"] == 200
        assert response["body"] == json.dumps(
            "Data successfully processed and inserted"
        )

    # Test for handling KeyError (missing 'key' in the event)
    @patch("src.load_lambda.main.logger")
    def test_lambda_handler_key_error(self, mock_logger):
        # Mock logger methods to avoid AttributeError
        mock_logger.info = MagicMock()
        mock_logger.error = MagicMock()

        # Modify mock event to trigger KeyError
        invalid_event = {
            "Records": [
                {
                    "s3": {
                        "object": {},  # Missing 'key'
                        "bucket": {"name": "test_bucket"},
                    }
                }
            ]
        }

        # Call the lambda handler
        response = lambda_handler(invalid_event, {})

        # Assert the error response
        assert response["statusCode"] == 400
        assert "Invalid event format" in response["body"]
