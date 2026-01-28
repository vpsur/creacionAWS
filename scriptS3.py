import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import os
import time

load_dotenv()

def conectarse():
    session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION"))

    s3 = session.client('s3')

    response = s3.list_buckets()
    print(response)
    return s3

def main():
    s3 = conectarse()


if __name__ == "__main__":
    main()