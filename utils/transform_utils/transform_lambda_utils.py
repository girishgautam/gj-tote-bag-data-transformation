import boto3
import pandas as pd
import io
from utils.extraction_utils.lambda_utils import get_s3_bucket_name


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


# bucket_name = get_s3_bucket_name("data-squid-ingest-bucket-")
# sales_order_df = convert_json_to_df_from_s3('sales_order', bucket_name)
# df_department = convert_json_to_df_from_s3('department', bucket_name)


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
            - A new column 'sales_record_id' is added with unique sequential integers starting from 1.
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
    fact_sales_order_df["sales_record_id"] = range(1, len(fact_sales_order_df) + 1)
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

# output = fact_sales_order(sales_order_df)
# print(output.head())


def dim_date(start='2022-11-03', end='2025-12-31'):
    calendar_range = pd.date_range(start, end)

    df = pd.DataFrame({'date_id': calendar_range})
    df['year'] = df.date_id.dt.year
    df['month'] = df.date_id.dt.month
    df['day'] = df.date_id.dt.day
    df['day_of_week'] = df.date_id.dt.day_of_week
    df['day_name'] = df.date_id.dt.day_name()
    df['month_name'] = df.date_id.dt.month_name()
    df['quarter'] = df.date_id.dt.quarter

    return df
