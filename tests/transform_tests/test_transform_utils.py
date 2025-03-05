from moto import mock_aws
from utils.lambda_utils import (
    convert_json_to_df_from_s3,
    dim_design,
    dim_staff,
    dim_location,
    dim_currency,
    dim_counterparty,
    dim_date,
    fact_sales_order,
    dataframe_to_parquet,
    format_data_to_json,
    create_filename,
    upload_to_s3,
)

import boto3
import pytest
import os
from datetime import datetime
from decimal import Decimal
import pandas as pd
import io


@pytest.fixture(scope="function", autouse=False)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class TestGetFile:
    def test_get_file_returns_dataframe(self):
        rows = [
            (1, "Alice", datetime(2025, 2, 26, 14, 33), Decimal("100.00")),
            (2, "Bob", datetime(2025, 2, 27, 15, 40), Decimal("200.50")),
        ]
        columns = ["id", "name", "timestamp", "amount"]
        with mock_aws():
            s3_client = boto3.client("s3")
            s3_client.create_bucket(
                Bucket="TestBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            result = format_data_to_json(rows, columns)
            timestamp_for_filename = datetime.now().strftime("%Y/%m/%d/%H:%M")
            timestamp_for_last_extracted = timestamp_for_filename.encode("utf-8")
            s3_client.put_object(
                Bucket="TestBucket",
                Key=f"test_table/last_extracted.txt",
                Body=timestamp_for_last_extracted,
            )
            filename = create_filename(
                table_name="test_table", time=timestamp_for_filename
            )
            upload_to_s3(data=result, bucket_name="TestBucket", object_name=filename)
            return_val = convert_json_to_df_from_s3(
                table="test_table", bucket_name="TestBucket"
            )
            print(type(return_val))
            assert return_val["id"][0] == 1
            assert return_val["name"][0] == "Alice"
            assert type(return_val) == pd.core.frame.DataFrame


class TestDimLocation:
    def test_dim_location_returns_dataframe(self):
        rows = [
            (
                1,
                "6826 Herzog Via",
                None,
                "Avon",
                "New Patienceburgh",
                "28441",
                "Turkey",
                "1803 637401",
            ),
            (
                2,
                "179 Alexie Cliffs",
                None,
                None,
                "Aliso Viejo",
                "99305-7380",
                "San Marino",
                "9621 880720",
            ),
        ]
        columns = [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
        with mock_aws():
            s3_client = boto3.client("s3")
            s3_client.create_bucket(
                Bucket="TestBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
            result = format_data_to_json(rows, columns)
            timestamp_for_filename = datetime.now().strftime("%Y/%m/%d/%H:%M")
            timestamp_for_last_extracted = timestamp_for_filename.encode("utf-8")
            s3_client.put_object(
                Bucket="TestBucket",
                Key=f"test_table/last_extracted.txt",
                Body=timestamp_for_last_extracted,
            )
            filename = create_filename(
                table_name="test_table", time=timestamp_for_filename
            )
            upload_to_s3(data=result, bucket_name="TestBucket", object_name=filename)
            return_val = convert_json_to_df_from_s3(
                table="test_table", bucket_name="TestBucket"
            )
            location = dim_location(return_val)

            assert location["location_id"][0] == 1
            assert type(location) == pd.core.frame.DataFrame
            assert location["address_line_1"][0] == "6826 Herzog Via"


class TestDimDesign:

    def test_dim_design_with_valid_input(self):
        """
        Test dim_design with a valid input DataFrame containing the required columns.
        """
        data = {
            "design_id": [1, 2, 3],
            "design_name": ["Design A", "Design B", "Design C"],
            "file_location": ["/path/a", "/path/b", "/path/c"],
            "file_name": ["file_a", "file_b", "file_c"],
            "extra_column": [10, 20, 30],  # Extra column to verify it is excluded.
        }
        df = pd.DataFrame(data)

        result = dim_design(df)

        expected_data = {
            "design_id": [1, 2, 3],
            "design_name": ["Design A", "Design B", "Design C"],
            "file_location": ["/path/a", "/path/b", "/path/c"],
            "file_name": ["file_a", "file_b", "file_c"],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(result, expected_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_design_missing_columns(self):
        """
        Test dim_design with a DataFrame missing some required columns to ensure it raises an error.
        """
        data = {
            "design_id": [1, 2, 3],
            "design_name": ["Design A", "Design B", "Design C"],
        }
        df = pd.DataFrame(data)

        with pytest.raises(KeyError):
            dim_design(df)

    def test_dim_design_empty_dataframe(self):
        """
        Test dim_design with an empty DataFrame to ensure it returns an empty DataFrame with the correct columns.
        """
        df = pd.DataFrame(
            columns=["design_id", "design_name", "file_location", "file_name"]
        )

        result = dim_design(df)

        expected_df = pd.DataFrame(
            columns=["design_id", "design_name", "file_location", "file_name"]
        )

        pd.testing.assert_frame_equal(result, expected_df)


class TestDimStaff:

    def test_dim_staff_valid_input(self):
        """
        Test dim_staff with valid input DataFrames to ensure it merges correctly.
        """
        staff_data = {
            "department_id": [1, 2],
            "staff_id": [101, 102],
            "first_name": ["Alice", "Bob"],
            "last_name": ["Smith", "Johnson"],
            "email_address": ["alice@example.com", "bob@example.com"],
        }
        department_data = {
            "department_id": [1, 2],
            "department_name": ["HR", "Engineering"],
            "location": ["New York", "London"],
        }

        staff_df = pd.DataFrame(staff_data)
        department_df = pd.DataFrame(department_data)

        result = dim_staff(staff_df, department_df)

        expected_data = {
            "staff_id": [101, 102],
            "first_name": ["Alice", "Bob"],
            "last_name": ["Smith", "Johnson"],
            "department_name": ["HR", "Engineering"],
            "location": ["New York", "London"],
            "email_address": ["alice@example.com", "bob@example.com"],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(result, expected_df)

    def test_dim_staff_missing_columns(self):
        """
        Test dim_staff with DataFrames missing required columns to ensure KeyError is raised.
        """
        staff_data = {
            "department_id": [1, 2],
            "staff_id": [101, 102],
            "first_name": ["Alice", "Bob"],
        }  # Missing 'last_name' and 'email_address'
        department_data = {
            "department_id": [1, 2],
            "department_name": ["HR", "Engineering"],
            "location": ["New York", "London"],
        }

        staff_df = pd.DataFrame(staff_data)
        department_df = pd.DataFrame(department_data)

        with pytest.raises(KeyError):
            dim_staff(staff_df, department_df)

    def test_dim_staff_empty_dataframes(self):
        """
        Test dim_staff with empty DataFrames to ensure it returns an empty DataFrame.
        """
        staff_df = pd.DataFrame(
            columns=[
                "department_id",
                "staff_id",
                "first_name",
                "last_name",
                "email_address",
            ]
        )
        department_df = pd.DataFrame(
            columns=["department_id", "department_name", "location"]
        )

        result = dim_staff(staff_df, department_df)

        expected_df = pd.DataFrame(
            columns=[
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        )

        pd.testing.assert_frame_equal(result, expected_df)

    def test_dim_staff_no_matching_department_ids(self):
        """
        Test dim_staff with no matching 'department_id' between the DataFrames to ensure it returns an empty DataFrame.
        """
        staff_data = {
            "department_id": [1],
            "staff_id": [101],
            "first_name": ["Alice"],
            "last_name": ["Smith"],
            "email_address": ["alice@example.com"],
        }
        department_data = {
            "department_id": [2],
            "department_name": ["Engineering"],
            "location": ["London"],
        }

        staff_df = pd.DataFrame(staff_data)
        department_df = pd.DataFrame(department_data)

        result = dim_staff(staff_df, department_df)

        expected_df = pd.DataFrame(
            columns=[
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        )

        assert len(result) == 0


class TestDimCurrency:

    def test_dim_currency_valid_input(self):
        """
        Test dim_currency with a valid input DataFrame that has valid currency codes.
        """
        data = {
            "currency_code": ["GBP", "USD", "EUR", "GBP"],
            "last_updated": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
            "created_at": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        }
        df = pd.DataFrame(data)

        result = dim_currency(df)

        expected_data = {
            "currency_code": ["GBP", "USD", "EUR", "GBP"],
            "currency_name": ["British Pound", "US Dollar", "Euro", "British Pound"],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), expected_df.reset_index(drop=True)
        )

    def test_dim_currency_invalid_input(self):
        """
        Test dim_currency with a DataFrame containing unmapped currency codes.
        """
        data = {
            "currency_code": ["GBP", "USD", "YEN"],
            "last_updated": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "created_at": ["2023-01-01", "2023-01-02", "2023-01-03"],
        }
        df = pd.DataFrame(data)

        result = dim_currency(df)

        expected_data = {
            "currency_code": ["GBP", "USD", "YEN"],
            "currency_name": ["British Pound", "US Dollar", None],
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), expected_df.reset_index(drop=True)
        )

    def test_dim_currency_missing_columns(self):
        """
        Test dim_currency with a DataFrame missing required columns.
        """
        data = {"currency_code": ["GBP", "USD", "EUR"]}
        df = pd.DataFrame(data)

        with pytest.raises(KeyError):
            dim_currency(df)

    def test_dim_currency_empty_dataframe(self):
        """
        Test dim_currency with an empty DataFrame to ensure it handles the case gracefully.
        """
        df = pd.DataFrame(columns=["currency_code", "last_updated", "created_at"])

        result = dim_currency(df)

        expected_df = pd.DataFrame(columns=["currency_code", "currency_name"])

        pd.testing.assert_frame_equal(
            result.reset_index(drop=True), expected_df.reset_index(drop=True)
        )


class TestDimCounterparty:
    def test_dim_counterparty_returns_dataframe(self):
        address_rows = [
            (
                15,
                "6826 Herzog Via",
                None,
                "Avon",
                "New Patienceburgh",
                "28441",
                "Turkey",
                "1803 637401",
            ),
            (
                28,
                "179 Alexie Cliffs",
                None,
                None,
                "Aliso Viejo",
                "99305-7380",
                "San Marino",
                "9621 880720",
            ),
        ]
        address_columns = [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
        counterparty_rows = [
            (1, "Fahey and Sons", 15, "Micheal Toy", "Mrs. Lucy Runolfsdottir"),
            (2, "Leannon, Predovic and Morar", 28, "Melba Sanford", "Jean Hane III"),
        ]
        counterparty_columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
        ]

        with mock_aws():
            s3_client = boto3.client("s3")
            s3_client.create_bucket(
                Bucket="TestBucket",
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )

            address_result = format_data_to_json(address_rows, address_columns)
            counterparty_result = format_data_to_json(
                counterparty_rows, counterparty_columns
            )

            timestamp_for_filename = datetime.now().strftime("%Y/%m/%d/%H:%M")
            timestamp_for_last_extracted = timestamp_for_filename.encode("utf-8")
            s3_client.put_object(
                Bucket="TestBucket",
                Key=f"test_address/last_extracted.txt",
                Body=timestamp_for_last_extracted,
            )
            s3_client.put_object(
                Bucket="TestBucket",
                Key=f"test_counterparty/last_extracted.txt",
                Body=timestamp_for_last_extracted,
            )

            address_filename = create_filename(
                table_name="test_address", time=timestamp_for_filename
            )
            counterparty_filename = create_filename(
                table_name="test_counterparty", time=timestamp_for_filename
            )

            upload_to_s3(
                data=address_result,
                bucket_name="TestBucket",
                object_name=address_filename,
            )
            upload_to_s3(
                data=counterparty_result,
                bucket_name="TestBucket",
                object_name=counterparty_filename,
            )

            address_return_val = convert_json_to_df_from_s3(
                table="test_address", bucket_name="TestBucket"
            )
            counterparty_return_val = convert_json_to_df_from_s3(
                table="test_counterparty", bucket_name="TestBucket"
            )

            dim_counterparty_result = dim_counterparty(
                address_return_val, counterparty_return_val
            )

            assert dim_counterparty_result["counterparty_id"][0] == 1
            assert type(dim_counterparty_result) == pd.core.frame.DataFrame
            assert (
                dim_counterparty_result["counterparty_legal_address_line_1"][0]
                == "6826 Herzog Via"
            )


class TestFactSalesOrder:

    @pytest.fixture
    def test_df(self):
        return pd.DataFrame(
            {
                "staff_id": [1, 2],
                "created_at": [
                    "2025-03-04 10:27:15.123456",
                    "2025-03-05 12:00:00.000000",
                ],
                "last_updated": [
                    "2025-03-04 10:28:15.123456",
                    "2025-03-05 13:00:00.000000",
                ],
            }
        )

    def test_fact_sales_order_datetime_format(self, test_df):
        """
        Tests if `fact_sales_order` extracts and formats date and time components correctly.
        """
        result = fact_sales_order(test_df)

        assert result["created_date"].tolist() == [
            datetime(2025, 3, 4).date(),
            datetime(2025, 3, 5).date(),
        ]
        assert result["created_time"].tolist() == [
            datetime(2025, 3, 4, 10, 27, 15, 123456).time(),
            datetime(2025, 3, 5, 12, 0, 0).time(),
        ]
        assert result["last_updated_date"].tolist() == [
            datetime(2025, 3, 4).date(),
            datetime(2025, 3, 5).date(),
        ]
        assert result["last_updated_time"].tolist() == [
            datetime(2025, 3, 4, 10, 28, 15, 123456).time(),
            datetime(2025, 3, 5, 13, 0, 0).time(),
        ]

    def test_fact_sales_order_structure(self, test_df):
        """Tests if fact_sales_order outputs the correct column structure."""
        result = fact_sales_order(test_df)

        assert "sales_staff_id" in result.columns
        assert "created_date" in result.columns
        assert "created_time" in result.columns
        assert "last_updated_date" in result.columns
        assert "last_updated_time" in result.columns

        assert "staff_id" not in result.columns
        assert "created_at" not in result.columns
        assert "last_updated" not in result.columns


class TestDataFrameToParquet:

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "created_at": [
                    "2025-03-04 10:27:15.123456",
                    "2025-03-05 12:00:00.654321",
                    "2025-03-06 14:30:45.987654",
                ],
            }
        )

    def test_parquet_conversion_output_type(self, sample_df):
        """Test if the function returns a bytes object."""
        parquet_data = dataframe_to_parquet(sample_df)
        assert isinstance(parquet_data, bytes)

    def test_empty_dataframe_conversion(self):
        """Test if the function works correctly with an empty DataFrame."""
        empty_df = pd.DataFrame()

        parquet_data = dataframe_to_parquet(empty_df)

        parquet_buffer = io.BytesIO(parquet_data)
        df_read_back = pd.read_parquet(parquet_buffer)

        assert df_read_back.empty


class TestDimDate:
    def test_dim_date_start_date_matches_date_start_date(self):
        result = dim_date()

        assert result["date_id"][0].strftime("%Y-%m-%d") == "2022-11-03"

    def test_dim_date_start_date_quarter_matches_start_date(self):
        result = dim_date()

        assert result["quarter"][0] == 4

    def test_dim_date_end_date_matches_date_specified_end_date(self):
        result = dim_date()

        assert result["date_id"][len(result) - 1].strftime("%Y-%m-%d") == "2025-12-31"

    def test_dim_date_end_date_quarter_matches_specified_end_date(self):
        result = dim_date()

        assert result["quarter"][len(result) - 1] == 4
