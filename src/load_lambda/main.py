import json
import logging
import urllib
from utils.lambda_utils import (
    insert_data_to_table,
    connect_to_warehouse,
    extract_tablenames_load,
    parquet_to_dataframe,
)


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda function to process reports from an S3 bucket and insert data into a data warehouse.
    """
    logger.info("Lambda function invoked with event: %s", json.dumps(event))

    # Validate event format
    try:
        if not event.get("Records"):
            raise KeyError("Missing 'Records' in event")

        # key = event['Records'][0]['s3']['object']['key']
        key = urllib.parse.unquote_plus(
            event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
        )
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        logger.info("Processing file %s from bucket %s", key, bucket_name)
    except KeyError as e:
        logger.error("Error parsing event: %s", str(e))
        return {
            "statusCode": 400,
            "body": json.dumps(f"Invalid event format: {str(e)}"),
        }

    # Extract table names from the report file
    try:
        transformed_table_names = extract_tablenames_load(bucket_name, key)
        logger.info("Extracted table names: %s", transformed_table_names)
    except Exception as e:
        logger.error("Error extracting table names: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error extracting table names: {str(e)}"),
        }

    # Define valid table names
    valid_table_names = [
        "dim_date",
        "dim_currency",
        "dim_location",
        "dim_counterparty",
        "dim_design",
        "dim_staff",
        "fact_sales_order",
    ]

    # Initialize database connection
    conn = None
    try:
        conn = connect_to_warehouse()
        logger.info("Successfully connected to the data warehouse")

        for table_name in transformed_table_names:
            if table_name in valid_table_names:
                try:
                    logger.info("Processing table: %s", table_name)
                    df = parquet_to_dataframe(table_name, bucket_name)
                    insert_data_to_table(conn, table_name, df)
                    logger.info("Successfully loaded table: %s", table_name)
                except Exception as e:
                    logger.error("Error loading table %s: %s", table_name, str(e))
                    continue  # Skip this table and move to the next one

    except Exception as e:
        logger.error("Error connecting to the warehouse: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error connecting to the data warehouse: {str(e)}"),
        }
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

    return {
        "statusCode": 200,
        "body": json.dumps("Data successfully processed and inserted"),
    }
