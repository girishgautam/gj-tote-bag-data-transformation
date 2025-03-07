from pg8000.native import Connection
import boto3
import datetime
from datetime import datetime
import pg8000
from botocore.exceptions import ClientError
from decimal import Decimal
import json
import io
import pandas as pd
import numpy as np


def upload_to_s3(data, bucket_name, object_name):
    """
    Uploads data to an S3 bucket.

    Args:
        data (str or bytes): The data to be uploaded to the S3 bucket.
        bucket_name (str): The name of the target S3 bucket.
        object_name (str): The name of the object to be created in the S3 bucket.

    Raises:
        ClientError: If the upload fails due to a client-side error with the AWS S3 service.

    Prints:
        A success message if the upload is successful, or an error message if the upload fails.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
        # print(f"Successfully uploaded {object_name} to {bucket_name}")
    except ClientError as e:
        # print(f"Failed to upload {object_name} to {bucket_name}: {e}")
        raise


def create_filename(table_name, time):
    """
    Generates a filename based on the current timestamp and the provided table name.

    Args:
        table_name (str): The name of the table to be included in the filename.

    Returns:
        str: A string representing the generated filename, formatted as
             "table_name/year/month/day/timestamp.json".
    """
    # timestamp = datetime.now().isoformat()
    # year = datetime.now().strftime("%Y")
    # month = datetime.now().strftime("%m")
    # day = datetime.now().strftime("%d")

    filename = f"{table_name}/{time}.json"
    return filename


def collect_credentials_from_AWS(sm_client, secret_id):
    """Returns credentials from AWS Secret Manager, function
    designed to be called from within connection_to_database function"""
    response = sm_client.get_secret_value(SecretId=secret_id)
    response_json = json.loads(response["SecretString"])

    return response_json


def connection_to_database(
    secret_id="arn:aws:secretsmanager:eu-west-2:195275662632:secret:totesys_database-RBM0fV",
):
    """
    Returns instance of pg8000 Connection for users to run database
    queries from totesys database and warehouse; secret_id will default to totesys database.

    To access the warehouse, pass secret_id argument: "arn:aws:secretsmanager:eu-west-2:195275662632:secret:database_warehouse-u8BUI3"
    """
    sm_client = boto3.client("secretsmanager")

    response = collect_credentials_from_AWS(sm_client, secret_id)

    user = response["username"]
    password = response["password"]
    database = response["dbname"]
    host = response["host"]
    port = response["port"]

    return Connection(
        user=user, password=password, database=database, host=host, port=port
    )


def check_for_data(s3_client, bucket_name):
    """This checks for presence of data in S3 Ingestion Bucket"""

    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return False if response["KeyCount"] < 1 else True


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
            # return obj.strftime("%Y/%m/%d/%H:%M")
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def format_data_to_json(rows, columns):
    """
    Convert data from rows and columns into a JSON-formatted bytes object.

    Args:
        rows (list of tuple): A list of tuples containing the data to be converted.
        columns (list of str): A list of column names corresponding to the data in rows.

    Returns:
        bytes: A bytes object containing the JSON-formatted data.

    The function uses a custom JSON encoder (CustomEncoder) to handle non-serializable
    objects such as datetime and Decimal. The data is first converted into a list of
    dictionaries, where each dictionary represents a row of data with column names as keys.
    The resulting list of dictionaries is then serialized into JSON and returned as a
    UTF-8 encoded bytes object.
    """
    data = [dict(zip(columns, row)) for row in rows]

    json_buffer = io.StringIO()
    json.dump(data, json_buffer, cls=CustomEncoder)

    json_buffer.seek(0)

    return json_buffer.getvalue().encode("utf-8")


def get_s3_bucket_name(bucket_prefix):
    # alternative method using env variables
    # load_dotenv()
    # bucket_name = os.getenv(bucket_key)
    # if not bucket_name:
    #     raise ValueError("bucket name not found")
    # return bucket_name

    """
    Retrieve the name of the  S3 bucket that starts with the specified prefix.
    ingest_bucket_prefix - "data-squid-ingest-bucket-"
    transform_bucket_prefix - "data-squid-transform-bucket-"

    Parameters:
    bucket_prefix (str): The prefix to match against the names of S3 buckets.

    Returns:
    str: The name of the first S3 bucket that starts with the given prefix.


    """

    s3_client = boto3.client("s3")

    response = s3_client.list_buckets()
    for bucket in response["Buckets"]:
        if bucket["Name"].startswith(bucket_prefix):
            return bucket["Name"]
    else:
        raise ValueError("Error: bucket prefix not found")


# Transform utils:


def convert_json_to_df_from_s3(table, bucket_name):
    """
    Fetches a JSON file from an S3 bucket, determined by the latest timestamp in 'last_extracted.txt',
    and converts its content into a pandas DataFrame.

    Args:
        table (str): Directory name in the S3 bucket containing the JSON files.
        bucket_name (str): Name of the S3 bucket.

    Returns:
        pd.DataFrame: DataFrame created from the JSON file's content.

    Notes:
        - Uses 'last_extracted.txt' to find the latest timestamp.
        - Requires `boto3` for S3 access and `pandas` for processing.
        - JSON files must be compatible with pandas' `read_json`.
    """
    s3_client = boto3.client("s3")
    last_extracted_obj = s3_client.get_object(
        Bucket=bucket_name, Key=f"{table}/last_extracted.txt"
    )
    last_extracted_time = last_extracted_obj["Body"].read().decode("utf-8")
    json_file_obj = s3_client.get_object(
        Bucket=bucket_name, Key=f"{table}/{last_extracted_time}.json"
    )
    json_file_str = json_file_obj["Body"].read().decode("utf-8")
    json_file_io = io.StringIO(json_file_str)
    df = pd.read_json(json_file_io)
    return df


def dim_design(df):
    """
    Extracts a subset of columns from the input Design DataFrame to create the `dim_design` DataFrame.

    This function selects the following columns from the input DataFrame:
    - 'design_id'
    - 'design_name'
    - 'file_location'
    - 'file_name'

    The resulting DataFrame is used for design-related information and can be further processed or analyzed.

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame containing design-related data.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the columns: 'design_id', 'design_name',
        'file_location', and 'file_name'.
    """

    dim_design_df = df[["design_id", "design_name", "file_location", "file_name"]]

    return dim_design_df


def dim_staff(df_1, df_2):
    """
    Merges the staff and department tables to create a dimensional staff DataFrame.

    Parameters:
    -----------
    staff_df : pandas.DataFrame
        The staff table containing staff-related information.
    department_df : pandas.DataFrame
        The department table containing department-related information.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the merged data with the following columns:
        - 'first_name'
        - 'last_name'
        - 'department_name'
        - 'location'
        - 'email_address'
    """
    try:
        dim_staff_df = pd.merge(df_1, df_2, on="department_id", how="inner")
        dim_staff_df = dim_staff_df[
            [
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        ]

        return dim_staff_df
    except KeyError:
        raise


def dim_location(df):

    dim_location_df = df[
        [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
    ]
    dim_location_df.rename(columns={"address_id": "location_id"}, inplace=True)

    return dim_location_df


def dim_counterparty(df1, df2):

    dim_counterparty_df = df1.merge(
        df2, left_on="address_id", right_on="legal_address_id"
    )
    dim_counterparty_df = dim_counterparty_df[
        [
            "counterparty_id",
            "counterparty_legal_name",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
    ]
    dim_counterparty_df.rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        },
        inplace=True,
    )

    return dim_counterparty_df


def dim_currency(df):
    """
    Transforms a DataFrame by mapping currency codes to currency names and removing unnecessary columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing a 'currency_code' column and other related data.

    Returns:
        pd.DataFrame: A transformed DataFrame with the following changes:
            - A new column, 'currency_name', is added by mapping 'currency_code' to human-readable currency names.
            - The 'last_updated' and 'created_at' columns are dropped from the DataFrame.

    """
    dim_currency_df = df
    currency_map = {"GBP": "British Pound", "USD": "US Dollar", "EUR": "Euro"}
    dim_currency_df["currency_name"] = dim_currency_df["currency_code"].map(
        currency_map
    )
    dim_currency_df.drop(columns=["last_updated", "created_at"], inplace=True)

    return dim_currency_df


def fact_sales_order(df):
    """
    Transforms a DataFrame to prepare sales order data for further processing or analysis.

    Args:
        df (pd.DataFrame): Input DataFrame containing sales order data.
            Expected columns include:
            - 'staff_id': Identifier for the staff associated with the sales order.
            - 'created_at': Datetime string indicating when the record was created.
            - 'last_updated': Datetime string indicating when the record was last updated.

    Returns:
        pd.DataFrame: Transformed DataFrame with the following changes:
            - The 'staff_id' column is renamed to 'sales_staff_id'.
            - 'created_at' and 'last_updated' are converted to datetime objects with their date and time components
              split into separate columns:
                - 'created_date': Date part of the 'created_at' timestamp.
                - 'created_time': Time part of the 'created_at' timestamp.
                - 'last_updated_date': Date part of the 'last_updated' timestamp.
                - 'last_updated_time': Time part of the 'last_updated' timestamp.
            - The original 'created_at' and 'last_updated' columns are dropped.

    The function is intended for cleaning and standardizing sales order data to ensure consistent formats
    and enable further analysis or storage.
    """

    fact_sales_order_df = df

    fact_sales_order_df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

    fact_sales_order_df["created_at"] = pd.to_datetime(df["created_at"], format="mixed")
    fact_sales_order_df["created_date"] = fact_sales_order_df["created_at"].dt.date
    fact_sales_order_df["created_time"] = fact_sales_order_df["created_at"].dt.time

    fact_sales_order_df["last_updated"] = pd.to_datetime(
        df["last_updated"], format="mixed"
    )
    fact_sales_order_df["last_updated_date"] = fact_sales_order_df[
        "last_updated"
    ].dt.date
    fact_sales_order_df["last_updated_time"] = fact_sales_order_df[
        "last_updated"
    ].dt.time

    fact_sales_order_df.drop(columns=["created_at", "last_updated"], inplace=True)

    return fact_sales_order_df


def dim_date(start="2022-11-03", end="2025-12-31"):
    calendar_range = pd.date_range(start, end)

    df = pd.DataFrame({"date_id": calendar_range})
    df["year"] = df.date_id.dt.year
    df["month"] = df.date_id.dt.month
    df["day"] = df.date_id.dt.day
    df["day_of_week"] = df.date_id.dt.day_of_week
    df["day_name"] = df.date_id.dt.day_name()
    df["month_name"] = df.date_id.dt.month_name()
    df["quarter"] = df.date_id.dt.quarter

    return df


def dataframe_to_parquet(df):
    """
    Convert a pandas DataFrame to Parquet format and return it as bytes.

    Args:
        df (pd.DataFrame): The pandas DataFrame to convert.

    Returns:
        bytes: The Parquet data in bytes.
    """
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    return parquet_buffer.getvalue()


def dim_date(start="2022-11-03", end="2025-12-31"):
    calendar_range = pd.date_range(start, end)

    df = pd.DataFrame({"date_id": calendar_range})
    df["year"] = df.date_id.dt.year
    df["month"] = df.date_id.dt.month
    df["day"] = df.date_id.dt.day
    df["day_of_week"] = df.date_id.dt.day_of_week
    df["day_name"] = df.date_id.dt.day_name()
    df["month_name"] = df.date_id.dt.month_name()
    df["quarter"] = df.date_id.dt.quarter

    return df


def create_filename_for_parquet(table_name, time):
    """
    Generates a filename based on the current timestamp and the provided table name.

    Args:
        table_name (str): The name of the table to be included in the filename.

    Returns:
        str: A string representing the generated filename, formatted as
             "table_name/year/month/day/timestamp.pqt".
    """
    # timestamp = datetime.now().isoformat()
    # year = datetime.now().strftime("%Y")
    # month = datetime.now().strftime("%m")
    # day = datetime.now().strftime("%d")

    filename = f"{table_name}/{time}.pqt"
    return filename


# load utils


def parquet_to_dataframe(s3_client, bucket, table):
    """
    Fetches a parquet file, for a given table, from the transform S3 bucket
    and converts the parquet file to a pandas dataframe

    args:
      s3_client is an AWS S3 client
      bucket is S3 bucket name where transformed data is stored as parquet files
      table is name of the database table

    returns:
      df: the last extracted parquet file converted to pandas dataframe
    """
    last_extracted_obj = s3_client.get_object(
        Bucket=bucket, Key=f"{table}/last_extracted.txt"
    )
    last_extracted_time = last_extracted_obj["Body"].read().decode("utf-8")

    s3_response = s3_client.get_object(
        Bucket=bucket,
        Key=f"{table}/{last_extracted_time}.pqt",
    )
    parquet_bytes_stream = s3_response["Body"].read()
    buffer = io.BytesIO(parquet_bytes_stream)
    df = pd.read_parquet(buffer)

    return df


def connect_to_warehouse():
    """
    Establishes a connection to the data warehouse using credentials stored in AWS Secrets Manager.

    This function retrieves the database credentials from AWS Secrets Manager, and then uses
    these credentials to establish and return a connection to the data warehouse.

    Returns:
        pg8000.Connection: A connection object to the data warehouse.
    """

    secret_id = (
        "arn:aws:secretsmanager:eu-west-2:195275662632:secret:database_warehouse-u8BUI3"
    )
    sm_client = boto3.client("secretsmanager")

    secret = collect_credentials_from_AWS(sm_client, secret_id)

    conn = pg8000.connect(
        user=secret["username"],
        database=secret["dbname"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
    )
    return conn


def insert_data_to_table(conn, table_name, df):
    """
    Inserts data from a DataFrame into a specified database table, handling conflicts by doing nothing.

    Args:
        conn (pg8000.Connection): Database connection object.
        table_name (str): Name of the table to insert data into.
        df (pandas.DataFrame): DataFrame containing the data to insert.

    Example:
        conn = connect_to_warehouse()
        df = pd.DataFrame({
            'sales_record_id': [1, 2, 3],
            'column1': ['value1', 'value2', 'value3'],
            'column2': ['value4', 'value5', 'value6']
        })
        insert_data_to_table(conn, 'your_table_name', df)
    """

    cursor = conn.cursor()
    for index, row in df.iterrows():
        columns = ", ".join(df.columns)
        conflict_column = df.columns[0]
        placeholders = ", ".join(["%s"] * len(row))
        query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_column}) DO NOTHING
        """

        row_data = tuple(row)

        try:
            cursor.execute(query, row_data)
            print(f"Inserted row {index + 1}")
        except Exception as e:
            print(f"Error inserting row {index + 1}: {e}")
    conn.commit()
    cursor.close()


def extract_tablenames_load(bucket_name, report_file):
    """
    Retrieves the list of updated table names from a report file in an S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        report_file (str): Key (file path) of the report file in the S3 bucket.

    Returns:
        list: Names of the updated tables.
    """

    s3_client = boto3.client("s3")

    report_file_obj = s3_client.get_object(Bucket=bucket_name, Key=report_file)
    report_file_str = report_file_obj["Body"].read().decode("utf-8")
    report_file = json.loads(report_file_str)
    tables = report_file["updated_tables"]
    return tables


# bucket_name = get_s3_bucket_name("data-squid-ingest-bucket-")
# df_currency = convert_json_to_df_from_s3('currency', bucket_name)
# dim_currency_df = dim_currency(df_currency)
# # print(dim_currency_df.head())
# conn = connect_to_warehouse()
# insert_data_to_table(conn, 'dim_currency', dim_currency_df)

# cursor = conn.cursor()
# query = f"DELETE FROM {'dim_currency'}"
# cursor.execute(query)
# conn.commit()
