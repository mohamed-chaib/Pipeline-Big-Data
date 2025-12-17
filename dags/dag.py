import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from datetime import datetime
from airflow.decorators import dag, task

from modules.minio_client import download_file_as_dataframe
from modules.postgres import upload_to_postgres
from modules.preprocess import advanced_cleaning, normalise_and_encodage, aggregate_df


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def pipeline_Big_Data():
    @task
    def extract_from_minio(bucket_name,key):
        """Extracts a CSV file from Minio and returns it ."""
        return download_file_as_dataframe(bucket_name, key)

    @task
    def transform_data(df):
        cleaned_df = advanced_cleaning(df)
        transformed_df = normalise_and_encodage(cleaned_df)
        return transformed_df

    @task
    def aggregate_data(df):
        return aggregate_df(df)
    @task
    def load_to_postgres(data,table_name):
        return upload_to_postgres(data , table_name)

    # --- Flow Definition ---
    raw_data = extract_from_minio(bucket_name='mybucket',key='data/remote_file.csv')
    transformed_data = transform_data(raw_data)
    aggregated_data = aggregate_data(transformed_data)
    load_to_postgres(aggregated_data,table_name='cleaned_data')


# Instantiate the DAG
minio_to_postgres_etl_dag = pipeline_Big_Data()