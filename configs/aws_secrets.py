import boto3
from botocore.exceptions import ClientError
import json
import os
from dotenv import load_dotenv

def get_secret():

    load_dotenv()

    secret_name = os.getenv("SECRET_NAME")
    region_name = os.getenv("REGION_NAME")

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])

    return secret

