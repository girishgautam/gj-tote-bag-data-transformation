from utils.lambda_utils import parquet_to_dataframe
import pytest
import pandas as pd
import io
from unittest.mock import patch, MagicMock
from utils.lambda_utils import connect_to_warehouse, insert_data_to_table


class TestParquetToDataframe:

    def test_returns_dataframe(self):
        byte_stream = io.BytesIO()
        df = pd.DataFrame.from_dict({"key": ["value"]})
        pqt = df.to_parquet(byte_stream, index=False)
        expected_output = parquet_to_dataframe(byte_stream)
        expected_result = pd.DataFrame.from_dict({"key": ["value"]})
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

    def test_insert_data_to_table(self):
        """
        Tests the insert_data_to_table function by mocking the database connection and cursor.

        This test verifies that the insert_data_to_table function correctly constructs and executes
        SQL INSERT queries for each row in the DataFrame, and handles conflicts on the 'sales_record_id' column.

        Args:
            self: The instance of the test case.

        """

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value

        data = {
            "sales_record_id": [1, 2, 3],
            "column1": ["value1", "value2", "value3"],
            "column2": ["value4", "value5", "value6"],
        }
        df = pd.DataFrame(data)

        insert_data_to_table(mock_conn, "test_table", df)

        expected_calls = [
            (
                "INSERT INTO test_table (sales_record_id, column1, column2) VALUES (%s, %s, %s) ON CONFLICT (sales_record_id) DO NOTHING;",
                (1, "value1", "value4"),
            ),
            (
                "INSERT INTO test_table (sales_record_id, column1, column2) VALUES (%s, %s, %s) ON CONFLICT (sales_record_id) DO NOTHING;",
                (2, "value2", "value5"),
            ),
            (
                "INSERT INTO test_table (sales_record_id, column1, column2) VALUES (%s, %s, %s) ON CONFLICT (sales_record_id) DO NOTHING;",
                (3, "value3", "value6"),
            ),
        ]
        for call in expected_calls:
            mock_cursor.execute.assert_any_call(*call)

        mock_cursor.close.assert_called_once()

        mock_conn.cursor.assert_called_once()
