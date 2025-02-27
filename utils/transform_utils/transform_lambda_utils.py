import boto3

def get_filename(tablename):
    s3_client = boto3.client('s3')
    result = s3_client.list_objects_v2(Bucket='data-squid-ingest-bucket-20250225123034817500000001', Prefix=tablename)
    print(result['Contents'])



get_filename("counterparty")