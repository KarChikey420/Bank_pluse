import boto3
import os
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
from time import sleep

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

def upload_to_s3():
    chunk_size = 10000
    bucket_name = "bank-bucket-kartikey"
    s3_folder = "bankpulse/chunks"

    for i, chunk in enumerate(pd.read_csv('transactions.csv', chunksize=chunk_size)):
        buffer = StringIO()
        chunk.to_csv(buffer, index=False)
        buffer.seek(0)

        filename = f"chunk_{i}.csv"
        key = f"{s3_folder}/{filename}"

        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue()
        )

        print(f"Uploaded {filename} to S3 at {key}")
        sleep(1) 

upload_to_s3()
