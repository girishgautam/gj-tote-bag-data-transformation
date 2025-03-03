from moto import mock_aws
from utils.transform_utils.transform_lambda_utils import\
    convert_json_to_df_from_s3,\
        dim_design,\
            dim_staff, dim_location
from utils.extraction_utils.lambda_utils import format_data_to_json, create_filename, upload_to_s3
import boto3
import pytest
import os
from datetime import datetime
from decimal import Decimal
import pandas as pd

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
            return_val = convert_json_to_df_from_s3(table='test_table', bucket_name='TestBucket')
            print(type(return_val))
            assert return_val['id'][0] == 1
            assert return_val['name'][0] == 'Alice'
            assert type(return_val) == pd.core.frame.DataFrame



class TestDimLocation:
    def test_dim_location_returns_dataframe(self):
        rows = [
            (1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401'),
            (2, '179 Alexie Cliffs', None, None, 'Aliso Viejo', '99305-7380', 'San Marino', '9621 880720')
        ]
        columns = ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
        with mock_aws():
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket='TestBucket', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            result = format_data_to_json(rows, columns)
            timestamp_for_filename = datetime.now().strftime("%Y/%m/%d/%H:%M")
            timestamp_for_last_extracted = timestamp_for_filename.encode("utf-8")
            s3_client.put_object(Bucket='TestBucket', Key=f'test_table/last_extracted.txt', Body=timestamp_for_last_extracted)
            filename = create_filename(table_name='test_table', time=timestamp_for_filename)
            upload_to_s3(data=result, bucket_name='TestBucket', object_name=filename)
            return_val = convert_json_to_df_from_s3(table='test_table', bucket_name='TestBucket')
            location = dim_location(return_val)
            print(location)
            assert location['location_id'][0] == 1
            assert type(location) == pd.core.frame.DataFrame
            assert location['address_line_1'][0] == '6826 Herzog Via'


class TestDimDesign:

    def test_dim_design_with_valid_input(self):
        """
        Test dim_design with a valid input DataFrame containing the required columns.
        """
        data = {
            'design_id': [1, 2, 3],
            'design_name': ['Design A', 'Design B', 'Design C'],
            'file_location': ['/path/a', '/path/b', '/path/c'],
            'file_name': ['file_a', 'file_b', 'file_c'],
            'extra_column': [10, 20, 30]  # Extra column to verify it is excluded.
        }
        df = pd.DataFrame(data)

        result = dim_design(df)

        expected_data = {
            'design_id': [1, 2, 3],
            'design_name': ['Design A', 'Design B', 'Design C'],
            'file_location': ['/path/a', '/path/b', '/path/c'],
            'file_name': ['file_a', 'file_b', 'file_c']
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(result, expected_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_design_missing_columns(self):
        """
        Test dim_design with a DataFrame missing some required columns to ensure it raises an error.
        """
        data = {
            'design_id': [1, 2, 3],
            'design_name': ['Design A', 'Design B', 'Design C']
        }
        df = pd.DataFrame(data)

        with pytest.raises(KeyError):
            dim_design(df)

    def test_dim_design_empty_dataframe(self):
        """
        Test dim_design with an empty DataFrame to ensure it returns an empty DataFrame with the correct columns.
        """
        df = pd.DataFrame(columns=['design_id', 'design_name', 'file_location', 'file_name'])

        result = dim_design(df)

        expected_df = pd.DataFrame(columns=['design_id', 'design_name', 'file_location', 'file_name'])

        pd.testing.assert_frame_equal(result, expected_df)


class TestDimStaff:

    def test_dim_staff_valid_input(self):
        """
        Test dim_staff with valid input DataFrames to ensure it merges correctly.
        """
        staff_data = {
            'department_id': [1, 2],
            'first_name': ['Alice', 'Bob'],
            'last_name': ['Smith', 'Johnson'],
            'email_address': ['alice@example.com', 'bob@example.com']
        }
        department_data = {
            'department_id': [1, 2],
            'department_name': ['HR', 'Engineering'],
            'location': ['New York', 'London']
        }

        staff_df = pd.DataFrame(staff_data)
        department_df = pd.DataFrame(department_data)

        result = dim_staff(staff_df, department_df)

        expected_data = {
            'first_name': ['Alice', 'Bob'],
            'last_name': ['Smith', 'Johnson'],
            'department_name': ['HR', 'Engineering'],
            'location': ['New York', 'London'],
            'email_address': ['alice@example.com', 'bob@example.com']
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(result, expected_df)

    def test_dim_staff_missing_columns(self):
        """
        Test dim_staff with a missing column in the input DataFrames to ensure it raises KeyError.
        """
        staff_data = {
            'department_id': [1, 2],
            'first_name': ['Alice', 'Bob'],
            'email_address': ['alice@example.com', 'bob@example.com']  # 'last_name' is missing.
        }
        department_data = {
            'department_id': [1, 2],
            'department_name': ['HR', 'Engineering'],
            'location': ['New York', 'London']
        }

        staff_df = pd.DataFrame(staff_data)
        department_df = pd.DataFrame(department_data)

        with pytest.raises(KeyError):
            dim_staff(staff_df, department_df)

    def test_dim_staff_empty_dataframes(self):
        """
        Test dim_staff with empty input DataFrames to ensure it returns an empty DataFrame with the correct columns.
        """
        staff_df = pd.DataFrame(columns=['department_id', 'first_name', 'last_name', 'email_address'])
        department_df = pd.DataFrame(columns=['department_id', 'department_name', 'location'])

        result = dim_staff(staff_df, department_df)

        expected_df = pd.DataFrame(columns=['first_name', 'last_name', 'department_name', 'location', 'email_address'])

        pd.testing.assert_frame_equal(result, expected_df)

    def test_dim_staff_no_matching_department_ids(self):
        """
        Test dim_staff with no matching 'department_id' between the DataFrames to ensure it returns an empty DataFrame.
        """
        staff_data = {
            'department_id': [1],
            'first_name': ['Alice'],
            'last_name': ['Smith'],
            'email_address': ['alice@example.com']
        }
        department_data = {
            'department_id': [2], 
            'department_name': ['Engineering'],
            'location': ['London']
        }

        staff_df = pd.DataFrame(staff_data)
        department_df = pd.DataFrame(department_data)

        result = dim_staff(staff_df, department_df)

        expected_df = pd.DataFrame(columns=['first_name', 'last_name', 'department_name', 'location', 'email_address'])

        pd.testing.assert_frame_equal(result, expected_df)
