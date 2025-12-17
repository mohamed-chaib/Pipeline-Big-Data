import io
import pandas as pd
from sqlalchemy import create_engine
import os

def get_postgres_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/{os.environ.get('POSTGRES_DB')}"
    )
def upload_to_postgres(df, table_name):
    engine = get_postgres_engine()
    df.to_sql(table_name, engine, if_exists="replace", index=False)
