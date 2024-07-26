import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from datetime import datetime
import hashlib
from typing import Optional, Union
from utils import generate_timestamp, compute_md5_hash

class S3Handler:
    def __init__(self, bucket_name: str, aws_access_key_id: Optional[str] = None, aws_secret_access_key: Optional[str] = None, region_name: Optional[str] = None):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def upload_file(self, file_name: str, object_name: Optional[str] = None, raise_exception: bool = False) -> None:
        """
        Uploads a local file to S3 and appends a timestamp to the object name.
        """
        try:
            if object_name is None:
                object_name = file_name
            timestamp = generate_timestamp()
            object_name_with_timestamp = f"{object_name}_{timestamp}"
            self.s3_client.upload_file(file_name, self.bucket_name, object_name_with_timestamp)
            print(f"File {file_name} uploaded to {object_name_with_timestamp}.")
        except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
            print(f"Failed to upload {file_name}: {e}")
            if raise_exception:
                raise

    def upload_object(self, obj: bytes, object_name: str, raise_exception: bool = False) -> None:
        """
        Uploads an in-memory object to S3 and appends a timestamp to the object name.
        """
        try:
            timestamp = generate_timestamp()
            object_name_with_timestamp = f"{object_name}_{timestamp}"
            self.s3_client.put_object(Bucket=self.bucket_name, Key=object_name_with_timestamp, Body=obj)
            print(f"Object uploaded to {object_name_with_timestamp}.")
        except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
            print(f"Failed to upload object: {e}")
            if raise_exception:
                raise

    def download_file(self, object_name: str, save_to_local: Optional[str] = None, raise_exception: bool = False) -> Optional[bytes]:
        """
        Downloads a file from S3 to RAM and optionally saves it locally.
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_name)
            data = response['Body'].read()
            if save_to_local:
                with open(save_to_local, 'wb') as file:
                    file.write(data)
                print(f"File {object_name} downloaded to {save_to_local}.")
            return data
        except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
            print(f"Failed to download {object_name}: {e}")
            if raise_exception:
                raise
            return None

    def download_etag(self, object_name: str, raise_exception: bool = False) -> Optional[str]:
        """
        Downloads a file's ETag from S3.
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=object_name)
            etag = response['ETag']
            print(f"ETag for {object_name} is {etag}.")
            return etag
        except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
            print(f"Failed to get ETag for {object_name}: {e}")
            if raise_exception:
                raise
            return None

    def has_file_changed(self, object_name: str, obj: bytes, raise_exception: bool = False) -> bool:
        """
        Compares the S3 ETag to the in-memory object's MD5 hash to determine if the file has changed.
        """
        try:
            etag = self.download_etag(object_name, raise_exception=raise_exception)
            if etag:
                md5_hash = compute_md5_hash(obj)
                etag_cleaned = etag.strip('"')  # Remove quotes from etag
                return etag_cleaned != md5_hash
            return False
        except Exception as e:
            print(f"Error in comparing file hash: {e}")
            if raise_exception:
                raise
            return False
