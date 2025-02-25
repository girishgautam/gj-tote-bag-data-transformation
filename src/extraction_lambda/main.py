import pg8000.native
import boto3
from botocore.exceptions import ClientError
import json
from utils.extraction_utils.lambda_utils import upload_to_s3, default_converter


def extract_data(s3_client, conn):
    s3_client = boto3.client('s3')
    # table_names = ['address', 'counterparty', 'design', 'sales_order'
    #                'transaction', 'payment', 'payment_type',
    #                'currency', 'staff', 'department', 'purchase_order']
    table_names = ['address']


    for table in table_names:
        query = f'SELECT * FROM {table} LIMIT 2'
        rows = conn.run(query)
        columns = [col["name"] for col in conn.columns]

        data = [dict(zip(columns, row)) for row in rows]
        data_json = json.dumps(data, default=default_converter)
        # filename = util_filenam(table)

        upload_to_s3(data_json, 'your-bucket-name', f'{table}.json')

