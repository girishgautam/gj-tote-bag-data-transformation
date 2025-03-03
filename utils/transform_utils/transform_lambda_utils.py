import boto3
import json
import pandas as pd
import io
from utils.extraction_utils.lambda_utils import get_s3_bucket_name
from pprint import pprint


def convert_json_to_df_from_s3(table, bucket_name):
    s3_client = boto3.client('s3')
    last_extracted_obj = s3_client.get_object(Bucket=bucket_name, Key=f'{table}/last_extracted.txt')
    last_extracted_time = last_extracted_obj['Body'].read().decode('utf-8')
    json_file_obj = s3_client.get_object(Bucket=bucket_name, Key=f'{table}/{last_extracted_time}.json')
    json_file_str = json_file_obj['Body'].read().decode('utf-8')
    json_file_io = io.StringIO(json_file_str)
    df = pd.read_json(json_file_io)
    return df

bucket_name = get_s3_bucket_name("data-squid-ingest-bucket-")

df_currency = convert_json_to_df_from_s3('currency', bucket_name)
print(df_currency.head()) #['currency_code'].unique()
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

    dim_design = df[['design_id', 'design_name', 'file_location', 'file_name']]

    # dim_design = dim_design.set_index('design_id')

    return dim_design


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
        dim_staff_df = pd.merge(df_1, df_2, on='department_id', how='inner')
        dim_staff_df = dim_staff_df[['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']]

        return dim_staff_df
    except KeyError:
        raise

def dim_location(df):

    dim_location = df[['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
    dim_location.rename(columns={'address_id': 'location_id'}, inplace=True)

    return dim_location



def dim_counterparty(df1, df2):
    
    dim_counterparty = df1.merge(df2, left_on='address_id', right_on='legal_address_id')
    dim_counterparty = dim_counterparty[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
    dim_counterparty.rename(columns={'address_line_1': 'counterparty_legal_address_line_1', 'address_line_2': 'counterparty_legal_address_line_2', 'district': 'counterparty_legal_district', 'city': 'counterparty_legal_city', 'postal_code': 'counterparty_legal_postal_code', 'country': 'counterparty_legal_country', 'phone': 'counterparty_legal_phone_number'}, inplace=True)

    return dim_counterparty


def dim_currency(df):
    dim_currency = df
    currency_map = {'GBP' : 'British Pound', 'USD' : 'US Dollar', 'EUR' : 'Euro'}
    dim_currency['currency_name'] = dim_currency['currency_code'].map(currency_map)
    dim_currency.drop(columns=['last_updated', 'created_at'], inplace=True)

    return dim_currency

