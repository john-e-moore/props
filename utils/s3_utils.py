import re
import boto3
from typing import Any
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError

class S3Utility:
    @staticmethod
    def get_latest_file_key(bucket: str, prefix: str) -> str:
        """
        Retrieves the most recent file (based on Last Modified date) from an S3 bucket for a given prefix.
        """
        s3_client = boto3.client('s3')
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            files = response.get('Contents', [])
        except ClientError as e:
            raise Exception(f"Failed to list files in S3: {e}")

        if not files:
            raise Exception("No files found under the specified bucket and prefix.")

        latest_file = max(files, key=lambda x: x['LastModified'])
        return latest_file['Key']

    @staticmethod
    def download_etag(bucket: str, key: str) -> str:
        s3_client = boto3.client('s3')
        try:
            s3_object = s3_client.head_object(Bucket=bucket, Key=key)
            s3_etag = s3_object['ETag'].strip('"')  # Remove double quotes from ETag
            return s3_etag
        except s3_client.exceptions.NoSuchKey:
            print("The object does not exist in S3.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    @staticmethod
    def upload_local_file_to_s3(local_file_path: str, bucket: str, key: str) -> None:
        """
        Uploads a file from the local file system to an S3 bucket.
        """
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(local_file_path, bucket, key)
            print(f"Successfully uploaded:\n{response}")
        except (ClientError, NoCredentialsError) as e:
            raise Exception(f"Failed to upload file to S3: {e}")

    @staticmethod
    def download_s3_file_to_local(local_file_path: str, bucket: str, key: str) -> None:
        """
        Downloads a file from an S3 bucket to the local file system.
        """
        s3_client = boto3.client('s3')
        try:
            s3_client.download_file(bucket, key, local_file_path)
        except ClientError as e:
            raise Exception(f"Failed to download file from S3: {e}")

    @staticmethod
    def upload_obj_s3(bucket: str, key: str, obj: str) -> None:
        """
        Uploads an object to S3.
        """
        s3_client = boto3.client('s3')
        try:
            response = s3_client.put_object(Bucket=bucket, Key=key, Body=obj)
            print(f"Successfully uploaded:\n{response}")
        except (ClientError, NoCredentialsError) as e:
            raise Exception(f"Failed to upload object to S3: {e}")

    @staticmethod
    def replace_timestamp_in_filename(filename: str) -> str:
        """
        Replaces an old timestamp in the file name with the current timestamp.
        The timestamp format is assumed to be YYYYMMDD.

        :param filename: The original file name with the old timestamp.
        :return: The new file name with the current timestamp.
        """
        current_timestamp = datetime.now().strftime("%Y%m%d")
        pattern = r'(\d{8})(?=\.\w+$)'  # Looks for 8 digits before the file extension
        # Replace the old timestamp with the current timestamp
        new_filename = re.sub(pattern, current_timestamp, filename)