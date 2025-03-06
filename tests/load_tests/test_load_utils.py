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
        df = pd.DataFrame({
            "sales_record_id": [1, 2],
            "column1": ["value1", "value2"],
            "column2": ["value3", "value4"]
        })

        table_name = "test_table"

        insert_data_to_table(mock_conn, table_name, df)

        # Assert cursor methods are called
        mock_conn.cursor.assert_called_once()
        mock_cursor = mock_conn.cursor.return_value

        # Check that execute() was called twice (for each row in df)
        assert mock_cursor.execute.call_count == len(df)

        # Verify the SQL query format
        expected_query = f"""
            INSERT INTO {table_name} (sales_record_id, column1, column2)
            VALUES (%s, %s, %s)
            ON CONFLICT (sales_record_id) DO NOTHING
        """
        expected_calls = [
            ((expected_query, (1, "value1", "value3")),),
            ((expected_query, (2, "value2", "value4")),)
        ]

        mock_cursor.execute.assert_has_calls(expected_calls, any_order=True)

        # Ensure commit() and cursor.close() are called
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
