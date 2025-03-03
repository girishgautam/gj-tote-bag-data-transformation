import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import json
from datetime import datetime
from utils.extraction_utils.lambda_utils import (
    upload_to_s3,
    check_for_data,
    format_data_to_json,
    create_filename,
    connection_to_database,
    get_s3_bucket_name
)
import logging


logging.basicConfig(level=logging.INFO)


s3_client = boto3.client("s3")
conn = connection_to_database()
bucket_name = get_s3_bucket_name("data-squid-ingest-bucket-")


def extract_data(s3_client, conn, bucket_name):
    """
    Extracts data from a database and uploads it to an S3 bucket.

    Parameters:
    s3_client (boto3.client): A boto3 S3 client instance used to
    interact with the S3 bucket.
    conn (database connection): A database connection object
    used to run queries and retrieve data.
    bucket_name (str): The name of the S3 bucket where the
    extracted data will be uploaded.

    Returns:
    tuple: A tuple containing the type of
    extraction ('Initial extraction' or 'Continuous extraction')
           and a message indicating which tables were extracted,
           or a message indicating that no tables were extracted.

    This function performs the following steps:
    1. Checks if data exists in the S3 bucket by
    calling the check_for_data function.
    2. If data exists, it retrieves the last extraction
    timestamp from 'last_extracted.txt' in the S3 bucket.
    3. Iterates through a list of table names and
    runs queries to extract data from each table.
       - For continuous extraction, it queries for data
       updated after the last extraction timestamp.
       - For initial extraction, it queries for all data in the table.
    4. Formats the extracted data to JSON and uploads it to the S3 bucket.
    5. Updates 'last_extracted.txt' in
    the S3 bucket with the current timestamp.
    6. Returns the type of extraction and a message
    indicating which tables were extracted,
    or a message indicating that no tables were extracted.
    """

    table_names = [
        "address",
        "counterparty",
        "design",
        "sales_order",
        "transaction",
        "payment",
        "payment_type",
        "currency",
        "staff",
        "department",
        "purchase_order",
    ]
    extracted_tables = []

    is_data = check_for_data(s3_client, bucket_name)

    timestamp = datetime.now()
    timestamp_for_filename = timestamp.strftime("%Y/%m/%d/%H:%M")
    timestamp_for_last_extracted = timestamp_for_filename.encode('utf-8')
    for table in table_names:
        if is_data:
            last_extracted_obj = s3_client.get_object(Bucket=bucket_name, Key=f'{table}/last_extracted.txt')
            last_extracted_str = last_extracted_obj['Body'].read().decode('utf-8')
            dt = datetime.strptime(last_extracted_str, "%Y/%m/%d/%H:%M")
            last_extracted = dt.isoformat()
            print(f"{table} last extraction date: {last_extracted}")
            query = f"SELECT * FROM {table} WHERE last_updated > '{last_extracted};'"
            extraction_type = 'Continuous extraction'

        else:
            query = f"SELECT * FROM {table};"
            s3_client.put_object(Bucket=bucket_name, Key=f'{table}/last_extracted.txt', Body=timestamp_for_last_extracted)
            extraction_type = 'Initial extraction'


        rows = conn.run(query)
        columns = [col["name"] for col in conn.columns]
        data_json = format_data_to_json(rows, columns)
        filename = create_filename(table, timestamp_for_filename)
        data_json_str = json.loads(data_json.decode('utf-8'))

        if data_json_str:
            s3_client.put_object(Bucket=bucket_name, Key=f'{table}/last_extracted.txt', Body=timestamp_for_last_extracted)
            upload_to_s3(data=data_json, bucket_name=bucket_name, object_name=filename)
            extracted_tables.append(table)



    if extracted_tables:
        return extraction_type, f"Tables extracted - {extracted_tables}"
    else:
        return 'No updates in the database, No Tables extracted'




def lambda_handler(event, context):
    try:
        s3_client = boto3.client("s3")
        logging.info("Succesfully created a s3 client")

    except NoCredentialsError:
        logging.error("AWS credentials not found. Unable to create S3 client")
        return {"result": "Failure", "error": "AWS credentials not found."}
    except ClientError as e:
        logging.error(f"Error creating S3 client: {e}")
        return {"result": "Failure", "error": "Error creating S3 client"}

    conn = connection_to_database()
    bucket_name = get_s3_bucket_name("data-squid-ingest-bucket-")

    try:
        result = extract_data(s3_client, conn, bucket_name)

        report = {
            "status": "Success",
            "message": result,
        }
        report_file_name = f"reports/{datetime.now().isoformat()}_success.json"

        s3_client.put_object(
            Body=json.dumps(report, indent=4),
            Bucket=bucket_name,
            Key=report_file_name,
        )

        logging.info(
            f"Extraction successful. Report stored in\
                S3: s3://{bucket_name}/{report_file_name}"
        )

        return {
            "result": "Success",
            "report_file": f"s3://{bucket_name}/{report_file_name}",
        }

    except ClientError as e:
        logging.error(f"Error updating last_extracted.txt: {e}")
        return {"result": "Failure",
                "error": "Error updating last_extracted.txt"}

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"result": "Failure", "error": "Unexpected error"}

    finally:
        conn.close()
        logging.info("Database connection closed.")
