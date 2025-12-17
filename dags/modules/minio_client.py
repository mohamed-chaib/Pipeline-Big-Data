import os
import boto3
import pandas as pd
import io
def create_minio_client():
    """
    Create a MinIO client using boto3
    """
    endpoint_url = f"http://{os.environ.get('MINIO_HOST', 'localhost')}:{os.environ.get('MINIO_PORT', 9000)}"
    access_key = os.environ.get('MINIO_USER', 'admin')
    secret_key = os.environ.get('MINIO_PASSWORD', 'password')

    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url
    )
    return client


def download_file_as_bytes(bucket_name, key):
    """
    Download file from MinIO and return bytes
    """
    client = create_minio_client()
    obj = client.get_object(Bucket=bucket_name, Key=key)
    return obj['Body'].read()


def download_file_as_dataframe(bucket_name, key):
    """
    Download CSV file from MinIO and return as pandas DataFrame
    """
    data_bytes = download_file_as_bytes(bucket_name, key)
    df = pd.read_csv(io.BytesIO(data_bytes))
    return df

