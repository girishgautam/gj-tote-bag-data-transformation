import boto3
import json
import pandas as pd
import io


def convert_json_to_df_from_s3(table, bucket_name):
    s3_client = boto3.client('s3')
    last_extracted_obj = s3_client.get_object(Bucket=bucket_name, Key=f'{table}/last_extracted.txt')
    last_extracted_time = last_extracted_obj['Body'].read().decode('utf-8')
    json_file_obj = s3_client.get_object(Bucket=bucket_name, Key=f'{table}/{last_extracted_time}.json')
    json_file_str = json_file_obj['Body'].read().decode('utf-8')
    json_file_io = io.StringIO(json_file_str)
    df = pd.read_json(json_file_io)
    return df


def dim_design(df):

    dim_design = df[['design_id', 'design_name', 'file_location', 'file_name']]

    # dim_design = dim_design.set_index('design_id')

    return dim_design


def dim_location(df):

    dim_location = df[['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]
    dim_location.rename(columns={'address_id': 'location_id'}, inplace=True)

    return dim_location



bucket_name = "data-squid-ingest-bucket-20250225123034817500000001"
df1 = convert_json_to_df_from_s3('address', bucket_name)
df2 = convert_json_to_df_from_s3('counterparty', bucket_name)

def dim_counterparty(df1, df2):
    return df2











print(dim_counterparty(df1, df2))

