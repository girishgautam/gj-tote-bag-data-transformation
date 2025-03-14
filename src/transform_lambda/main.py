import boto3
import json
from datetime import datetime
import logging
import urllib
from botocore.exceptions import ClientError, NoCredentialsError
from utils.lambda_utils import (
    convert_json_to_df_from_s3,
    dim_design,
    dim_date,
    dim_counterparty,
    dim_currency,
    dim_location,
    dim_staff,
    fact_sales_order,
    dataframe_to_parquet,
    create_filename_for_parquet,
    upload_to_s3,
    get_s3_bucket_name,
    check_for_data,
)


logger = logging.getLogger()
logger.setLevel("INFO")


def lambda_handler(event, context):
    try:
        s3_client = boto3.client("s3")
    except NoCredentialsError:
        logging.error("AWS credentials not found. Unable to create S3 client")
        return {"result": "Failure", "error": "AWS credentials not found."}
    except ClientError as e:
        logging.error(f"Error creating S3 client: {e}")
        return {"result": "Failure", "error": "Error creating S3 client"}

    timestamp = datetime.now()
    timestamp_for_filename = timestamp.strftime("%Y/%m/%d/%H:%M")
    timestamp_for_last_extracted = timestamp_for_filename.encode("utf-8")
    ingestion_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    report_file = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )
    transform_bucket_name = get_s3_bucket_name("data-squid-transform")

    # These variable names are for use in creation of dim tables which require 2 dataframes in order to be created.
    # The dataframe is stored in {tablename}_df and the {tablename}_df_exists is changed to True.
    # The {tablename}_df_exists variables are required as dataframes themselves do not have boolean values.
    address_df = None
    address_df_exists = False
    counterparty_df = None
    counterparty_df_exists = False

    department_df = None
    department_df_exists = False
    staff_df = None
    staff_df_exists = False

    # This variable has been created for the purposes of ensuring that the fact_sales_order table is appended to last
    # in the transformed_tables list. This is important for how we process the relevant tables in the load_lambda function.
    fact_sales_order_table_created = False
    transformed_tables = []

    # Checks for presence of data in transform bucket, if no data present we need to create dim_date table
    # If data is present, we do not need to create this table again as the dates will not change and are for a set period
    if check_for_data(s3_client, transform_bucket_name) == False:
        dim_date_tab = dim_date()
        parquet_file = dataframe_to_parquet(dim_date_tab)
        filename = create_filename_for_parquet("dim_date", timestamp_for_filename)
        upload_to_s3(
            data=parquet_file, bucket_name=transform_bucket_name, object_name=filename
        )
        s3_client.put_object(
            Bucket=transform_bucket_name,
            Key="dim_date/last_transformed.txt",
            Body=timestamp_for_last_extracted,
        )
        transformed_tables.append("dim_date")

    tables = extract_tablenames(s3_client, ingestion_bucket_name, report_file)
    

    # Iterates through all of the tables that have recently been ingested by the ingestion lambda function
    # and stores the converted dataframe to parquet file in the transform bucket.
    # If two tables are required for the formation of a dim table, we assign the {table} dataframe to a variable
    # and then call the relevant dataframe util with both relevant variable names as args.
    for table in tables:
        try:
            dataframe = convert_json_to_df_from_s3(table, ingestion_bucket_name)
        except ClientError as e:
            logger.warning("Invalid table name")

        if table == "design":
            dim_design_tab = dim_design(dataframe)
            parquet_file = dataframe_to_parquet(dim_design_tab)
            filename = create_filename_for_parquet("dim_design", timestamp_for_filename)
            upload_to_s3(
                data=parquet_file,
                bucket_name=transform_bucket_name,
                object_name=filename,
            )
            s3_client.put_object(
                Bucket=transform_bucket_name,
                Key="dim_design/last_transformed.txt",
                Body=timestamp_for_last_extracted,
            )
            transformed_tables.append("dim_design")
        elif table == "address":
            dim_location_tab = dim_location(dataframe)
            parquet_file = dataframe_to_parquet(dim_location_tab)
            filename = create_filename_for_parquet(
                "dim_location", timestamp_for_filename
            )
            upload_to_s3(
                data=parquet_file,
                bucket_name=transform_bucket_name,
                object_name=filename,
            )
            s3_client.put_object(
                Bucket=transform_bucket_name,
                Key="dim_location/last_transformed.txt",
                Body=timestamp_for_last_extracted,
            )
            transformed_tables.append("dim_location")
            address_df = dataframe
            address_df_exists = True
        elif table == "currency":
            dim_currency_tab = dim_currency(dataframe)
            parquet_file = dataframe_to_parquet(dim_currency_tab)
            filename = create_filename_for_parquet(
                "dim_currency", timestamp_for_filename
            )
            upload_to_s3(
                data=parquet_file,
                bucket_name=transform_bucket_name,
                object_name=filename,
            )
            s3_client.put_object(
                Bucket=transform_bucket_name,
                Key="dim_currency/last_transformed.txt",
                Body=timestamp_for_last_extracted,
            )
            transformed_tables.append("dim_currency")
        elif table == "sales_order":
            fact_sales_tab = fact_sales_order(dataframe)
            parquet_file = dataframe_to_parquet(fact_sales_tab)
            filename = create_filename_for_parquet(
                "fact_sales_order", timestamp_for_filename
            )
            upload_to_s3(
                data=parquet_file,
                bucket_name=transform_bucket_name,
                object_name=filename,
            )
            s3_client.put_object(
                Bucket=transform_bucket_name,
                Key="fact_sales_order/last_transformed.txt",
                Body=timestamp_for_last_extracted,
            )
            fact_sales_order_table_created = True
        elif table == "counterparty":
            counterparty_df = dataframe
            counterparty_df_exists = True
        elif table == "department":
            department_df = dataframe
            department_df_exists = True
        elif table == "staff":
            staff_df = dataframe
            staff_df_exists = True

    if address_df_exists and counterparty_df_exists:
        dim_counterparty_tab = dim_counterparty(address_df, counterparty_df)
        parquet_file = dataframe_to_parquet(dim_counterparty_tab)
        filename = create_filename_for_parquet(
            "dim_counterparty", timestamp_for_filename
        )
        upload_to_s3(
            data=parquet_file, bucket_name=transform_bucket_name, object_name=filename
        )
        s3_client.put_object(
            Bucket=transform_bucket_name,
            Key="dim_counterparty/last_transformed.txt",
            Body=timestamp_for_last_extracted,
        )
        transformed_tables.append("dim_counterparty")
    if department_df_exists and staff_df_exists:
        dim_staff_tab = dim_staff(department_df, staff_df)
        parquet_file = dataframe_to_parquet(dim_staff_tab)
        filename = create_filename_for_parquet("dim_staff", timestamp_for_filename)
        upload_to_s3(
            data=parquet_file, bucket_name=transform_bucket_name, object_name=filename
        )
        s3_client.put_object(
            Bucket=transform_bucket_name,
            Key="dim_staff/last_transformed.txt",
            Body=timestamp_for_last_extracted,
        )
        transformed_tables.append("dim_staff")

    if fact_sales_order_table_created:
        transformed_tables.append("fact_sales_order")

   

    # If transformed_tables list has been appended to, then we will create a report documenting the tables that have been transformed.
    # Else, we will return and log an error message that this lambda has been triggered erroneously.
    if transformed_tables:

        report = {
            "status": "Success",
            "transformed_tables": transformed_tables,
        }
        report_file_name = f"reports/{datetime.now().isoformat()}_success.json"

        s3_client.put_object(
            Body=json.dumps(report, indent=4),
            Bucket=transform_bucket_name,
            Key=report_file_name,
        )

        logger.info(
            f"Transformation successful. Report stored in\
                S3: s3://{transform_bucket_name}/{report_file_name}"
        )

        return {
            "result": "Success",
            "report_file": f"s3://{transform_bucket_name}/{report_file_name}",
        }
    else:
        logger.warning(
            "Lambda was called without valid tables. Extraction report should not have been created"
        )

        return "Lambda was called without valid tables. Extraction report should not have been created"


def extract_tablenames(s3_client, bucket_name, report_file):
    '''
    Retrieves the most recent report file from the ingest s3 bucket that contains a JSON formatted
    body. Returns the tablenames in a list stored on the key of "updated_tables", which the lambda handler uses
    to iterate through and perform relevant processing on each tablename stored in the list.
    '''
    report_file_obj = s3_client.get_object(Bucket=bucket_name, Key=report_file)
    report_file_str = report_file_obj["Body"].read().decode("utf-8")
    report_file = json.loads(report_file_str)
    tables = report_file["updated_tables"]
    return tables
