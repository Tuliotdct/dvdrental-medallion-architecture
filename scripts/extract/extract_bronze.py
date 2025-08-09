import pandas as pd
from sqlalchemy import inspect
from dotenv import load_dotenv
from credentials.rds_postgres_connection import get_connection
import boto3
import io
import os
import json
from datetime import datetime

load_dotenv()
bucket = os.getenv("BUCKET_NAME")
metadata_key = os.getenv("METADATA_KEY")
execution_date = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

engine = get_connection()
inspector = inspect(engine)
tables = inspector.get_table_names()
s3_client = boto3.client("s3")


def load_metadata():
    try:
        response = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        return json.load(response["Body"])
    except s3_client.exceptions.NoSuchKey:
        return {}

def save_metadata(metadata: dict):
    buffer = io.BytesIO()
    buffer.write(json.dumps(metadata).encode())
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=metadata_key, Body=buffer)

def export_table_to_s3(table: str, df: pd.DataFrame):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy")
    buffer.seek(0)

    key = f"bronze/{table}/{execution_date}/{table}-{execution_date}.parquet"
    s3_client.put_object(Body=buffer, Bucket=bucket, Key=key)

def get_last_update(df: pd.DataFrame, col: str) -> str:
    return str(df[col].max())


def load_bronze():
    metadata = load_metadata()

    for table in tables:
        last_update_col = "last_update"
        last_processed = metadata.get(table)

        if last_processed:
            query = f"SELECT * FROM {table} WHERE {last_update_col} > '{last_processed}'"
        else:
            query = f"SELECT * FROM {table}"

        df = pd.read_sql_query(query, con=engine)

        if df.empty:
            continue

        export_table_to_s3(table, df)
        metadata[table] = get_last_update(df, last_update_col)

    save_metadata(metadata)
    print("Bronze data extraction completed")

if __name__ == "__main__":
    load_bronze()

