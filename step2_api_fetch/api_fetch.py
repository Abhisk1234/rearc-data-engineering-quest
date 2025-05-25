import requests
import json
import boto3
from botocore.exceptions import ClientError

# S3 Configuration for LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

BUCKET_NAME = "bls-data"
S3_KEY = "api/datausa_population.json"
API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

def get_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Failed to fetch API data: {e}")
        return None

def upload_to_s3(bucket, key, data):
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
        print(f"Uploaded JSON to S3: s3://{bucket}/{key}")
    except ClientError as e:
        print(f"Failed to upload to S3: {e}")

def main():
    data = get_data(API_URL)
    if data:
        upload_to_s3(BUCKET_NAME, S3_KEY, data)

if __name__ == "__main__":
    main()
