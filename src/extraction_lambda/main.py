import pg8000.native
import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime
import time
from utils.extraction_utils.lambda_utils import upload_to_s3,\
    check_for_data, format_data_to_json, create_filename,\
        connection_to_database


def extract_data(s3_client, conn, bucket_name):
    table_names = ['address', 'counterparty', 'design', 'sales_order',
                   'transaction', 'payment', 'payment_type',
                   'currency', 'staff', 'department', 'purchase_order']
    extracted_tables = []
    start_time = time.time()

    is_data = check_for_data(s3_client, bucket_name)


    last_extracted = None
    if is_data:
        try:
            last_extracted_obj = s3_client.get_object(Bucket=bucket_name, Key='last_extracted.txt')
            last_extracted = last_extracted_obj['Body'].read().decode('utf-8')
            # print(f"Last extracted: {last_extracted}")
        except s3_client.exceptions.NoSuchKey:
            print("No last_extracted.txt file found. Performing initial extraction.")

    for table in table_names:
        if last_extracted:
            query = f"SELECT * FROM {table} WHERE last_updated > '{last_extracted}'"
            extraction_type = 'Continuous extraction'
        else:
            query = f"SELECT * FROM {table}"
            extraction_type = 'Initial extraction'


        rows = conn.run(query)
        columns = [col["name"] for col in conn.columns]
        data_json = format_data_to_json(rows, columns)
        filename = create_filename(table)
        data_json_str = json.loads(data_json.decode('utf-8'))

        if data_json_str:
            upload_to_s3(data_json, bucket_name, filename)
            extracted_tables.append(table)

    last_extracted = datetime.now().isoformat().encode('utf-8')
    s3_client.put_object(Bucket=bucket_name, Key='last_extracted.txt', Body=last_extracted)
    end_time = time.time()
    time_taken = end_time - start_time
    print(f'Time taken: {time_taken}')

    if extracted_tables:
        return extraction_type, f'Tables extracted - {extracted_tables}'
    else:
        return 'No updates in the database, No Tables extracted'


s3_client = boto3.client("s3")
conn = connection_to_database()
bucket_name = 'data-squid-ingest-bucket-20250225123034817500000001'
print(extract_data(s3_client, conn, bucket_name))
