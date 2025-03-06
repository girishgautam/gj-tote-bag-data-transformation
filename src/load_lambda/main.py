import json
import boto3
from utils.lambda_utils import insert_data_to_table,\
    connect_to_warehouse,\
        extract_tablenames


def lambda_handler(event, context):
    # key - parse the event and get the report filename
    # bucket_name - parse the event and get the bucket name
    # itereate the tablenames and pass each table through parquet_to_dataframe()
    # upload each table using insert_data_to_table()

    def lambda_handler(event, context):
        """
        Lambda function to process reports from an S3 bucket and insert data into a data warehouse.
        """
        # 1. Parse the event and get the report filename and bucket name
        try:
            key = event['Records'][0]['s3']['object']['key']
            bucket_name = event['Records'][0]['s3']['bucket']['name']
        except KeyError as e:
            print(f"Error parsing event: {e}")
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid event format')
            }

        transformed_table_names = extract_tablenames(bucket_name, key)

        # 2. list of transformed tables
        valid_table_names = ['dim_date',
                            'dim_currency',
                            'dim_location',
                            'dim_counterparty',
                            'dim_design',
                            'dim_staff',
                            'fact_sales_order']

        # 3. Upload each table to the data warehouse
        conn = None
        try:
            conn = connect_to_warehouse()
        except Exception as e:
            print(f"Error connecting to the warehouse: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps('Error connecting to the data warehouse')
            }

        for table_name in transformed_table_names:
            if table_name in valid_table_names:
                try:
                    df = parquet_to_dataframe(table_name, bucket_name)
                    insert_data_to_table(conn, table_name, df)
                except Exception as e:
                    print(f"Error loading table {table_name}: {e}")
                    continue  # Skip this table and move to the next one

        # 4. Close the database connection
        if conn:
            conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully processed and inserted')
        }
