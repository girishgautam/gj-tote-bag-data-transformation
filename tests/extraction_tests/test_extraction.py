from utils.extraction_utils.lambda_utils import collect_credentials_from_AWS, connection_to_database, check_for_data
from moto import mock_aws
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
            sm_client = boto3.client('secretsmanager')
            secret_value = {'user_id': 'test_user', 'password': 'test_password'}
            sm_client.create_secret(Name='Test_Name', SecretString=json.dumps(secret_value))

            response = collect_credentials_from_AWS(sm_client, secret_id='Test_Name')

            assert response['user_id'] == 'test_user'
            assert response['password'] == 'test_password'


class TestConnectionToDatabase:

    # Use the collect credentials to obtain database AWS Secret

    def test_connection_to_database_can_run_query(self):
        db = connection_to_database()
        query = 'SELECT * FROM payment LIMIT 2;'
        result = db.run(query)

        assert type(result) == list
        assert len(result) == 2

            

class TestCheckForData:
    
    def test_check_for_data_returns_false_if_bucket_empty(self):
        with mock_aws():
            #create mock s3 bucket
            s3_client = boto3.client('s3')
            #function checks for data in mock s3 bucket
            s3_client.create_bucket(Bucket='TestBucket', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            # call check for data
            result = check_for_data(s3_client, bucket_name='TestBucket')
    
            assert result == False


    def test_check_for_data_returns_true_if_bucket_occupied(self):
        with mock_aws():
            #create mock s3 bucket
            s3_client = boto3.client('s3')
            #function checks for data in mock s3 bucket
            s3_client.create_bucket(Bucket='TestBucket', CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
            #add data to mock s3 bucket
            s3_client.put_object(Body='Test.txt', Bucket='TestBucket', Key='TestKey')
            # call check for data
            result = check_for_data(s3_client, bucket_name='TestBucket')
            assert result == True