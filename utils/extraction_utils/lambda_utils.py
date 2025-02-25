from pg8000.native import Connection
import boto3
import json

def collect_credentials_from_AWS(sm_client, secret_id):

    response = sm_client.get_secret_value(SecretId=secret_id)
    response_json = json.loads(response["SecretString"])

    return response_json



def connection_to_database():

    sm_client = boto3.client('secretsmanager')
    secret_id = 'arn:aws:secretsmanager:eu-west-2:195275662632:secret:totesys_database-RBM0fV'

    response = collect_credentials_from_AWS(sm_client, secret_id)

    user = response['username']
    password = response['password']
    database = response['dbname']
    host = response['host']
    port = response['port']

    return Connection(user=user, password=password, database=database, host=host, port=port)



def check_for_data(s3_client, bucket_name):
    ''' This checks for presence of data in S3 Ingestion Bucket'''
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    print(response)
    return False if response['KeyCount'] < 1 else True

