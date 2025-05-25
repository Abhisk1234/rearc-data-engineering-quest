import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urljoin
from datetime import datetime
import email.utils
import json

# Constants

S3_KEY = "api/datausa_population.json"
API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
S3_BUCKET = "bls-data"
HEADERS = {
    "User-Agent": "Abhishek Nandgadkar (nandgadkar.abhishek@example.com) - Data Engineer, accessing BLS data for research purposes"
}

# Setup S3 client for LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://host.docker.internal:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

def create_bucket_if_not_exists(bucket_name, s3_client):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # Bucket does not exist, create it
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Error checking bucket: {e}")
            raise


def list_s3_files_with_metadata(bucket):
    """Return dict of {key: LastModified}"""
    try:
        create_bucket_if_not_exists("bls-data", s3)
        response = s3.list_objects_v2(Bucket=bucket)
        return {
            obj['Key']: obj['LastModified'] for obj in response.get('Contents', [])
        }
    except ClientError as e:
        print(f"Error listing S3 bucket: {e}")
        return {}

def get_source_files():
    """Scrape full URLs and clean keys from BLS directory listing"""
    response = requests.get(BASE_URL, headers=HEADERS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    file_map = {}
    for link in soup.find_all("a"):
        href = link.get("href")
        if href and not href.endswith("/"):
            full_url = urljoin(BASE_URL, href)
            key = href.lstrip("/")
            file_map[key] = full_url
    return file_map

def get_source_last_modified(url):
    """Get Last-Modified time of a remote file as datetime"""
    try:
        head = requests.head(url, headers=HEADERS)
        lm = head.headers.get("Last-Modified")
        if lm:
            return email.utils.parsedate_to_datetime(lm)
    except Exception as e:
        print(f"Warning: Could not get Last-Modified for {url}: {e}")
    return None

def sync_files():
    source_files = get_source_files()  # {key: url}
    s3_files = list_s3_files_with_metadata(S3_BUCKET)

    print(f"Found {len(source_files)} files in source")
    print(f"Found {len(s3_files)} files in S3")

    for key, file_url in source_files.items():
        source_modified = get_source_last_modified(file_url)
        s3_modified = s3_files.get(key)

        should_upload = False
        if key not in s3_files:
            print(f"New file: {key}")
            should_upload = True
        elif source_modified and s3_modified and source_modified > s3_modified:
            print(f"Updated file: {key} (source is newer)")
            should_upload = True
        else:
            print(f"Skipping: {key}")

        if should_upload:
            try:
                resp = requests.get(file_url, headers=HEADERS)
                resp.raise_for_status()
                s3.put_object(Bucket=S3_BUCKET, Key=key, Body=resp.content)
                print(f"Uploaded to S3: {key}")
            except Exception as e:
                print(f"Failed to upload {key}: {e}")

    # Delete removed files
    to_delete = set(s3_files.keys()) - set(source_files.keys())
    for key in to_delete:
        print(f"Deleting from S3 (not in source): {key}")
        s3.delete_object(Bucket=S3_BUCKET, Key=key)


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

def api_data_handling():
    data = get_data(API_URL)
    if data:
        upload_to_s3(S3_BUCKET, S3_KEY, data)


def lambda_handler(event, context):
    sync_files()
    api_data_handling()
    return {
        "statusCode": 200,
        "body": "Ingest function executed successfully"
    }

