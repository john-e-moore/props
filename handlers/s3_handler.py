import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from typing import Optional, Union
from utils.utils import generate_timestamp, compute_md5_hash

class S3Handler:
    def __init__(self, bucket_name: str, aws_access_key_id: Optional[str] = None, aws_secret_access_key: Optional[str] = None, region_name: Optional[str] = None):
        """
        Initializes the S3Handler with AWS credentials and the name of the bucket.

        :param bucket_name: The name of the AWS S3 bucket.
        :param aws_access_key_id: Optional AWS access key ID for authentication.
        :param aws_secret_access_key: Optional AWS secret access key for authentication.
        :param region_name: Optional AWS region where the bucket is hosted.
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def upload_file(self, file_name: str, base_key: str, file_extension: str, object_name: Optional[str] = None, raise_exception: bool = False) -> None:
        """
        Uploads a local file to S3 and appends a timestamp to the object name to ensure uniqueness.

        :param file_name: Path to the local file to be uploaded.
        :param object_name: The object name under which the file will be saved in the S3 bucket. If None, uses file_name.
        :param raise_exception: If True, raises any exception that occurs, otherwise prints the error.
        """
        if object_name is None:
            object_name = file_name
        timestamp = generate_timestamp()
        full_key = f"{base_key}/{object_name}_{timestamp}.{file_extension}"
        try:
            self.s3_client.upload_file(file_name, self.bucket_name, full_key)
            print(f"File {file_name} uploaded to {full_key}.")
        except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
            print(f"Failed to upload {file_name}: {e}")
            if raise_exception:
                raise

    def upload_object(self, obj: bytes, object_name: str, base_key: str, file_extension: str, raise_exception: bool = False) -> None:
        """
        Uploads an in-memory object (like bytes) to S3, appending a timestamp to ensure uniqueness.

        :param obj: The object (data) to be uploaded.
        :param object_name: The name for the object in S3.
        :param raise_exception: If True, raises any exception that occurs, otherwise prints the error.
        """
        timestamp = generate_timestamp()
        full_key = f"{base_key}/{object_name}_{timestamp}.{file_extension}"
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=full_key, Body=obj)
            print(f"Object uploaded to {full_key}.")
        except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
            print(f"Failed to upload object: {e}")
            if raise_exception:
                raise

    def download_file(self, object_name: str, save_to_local: Optional[str] = None, raise_exception: bool = False) -> Optional[bytes]:
        """
        Downloads a file from S3 to RAM and optionally saves it locally.

        :param object_name: The S3 path of the file to download.
        :param save_to_local: Local path to save the file. If None, the file is not saved.
        :param raise_exception: If True, raises any exception that occurs, otherwise prints the error.
        :return: The data as bytes if not saved locally, None if saved locally or if an error occurs.
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
        Downloads a file's ETag from S3 to check the integrity and version of the file.

        :param object_name: The S3 path of the file whose ETag is being retrieved.
        :param raise_exception: If True, raises any exception that occurs, otherwise prints the error.
        :return: The ETag as a string if successful, None otherwise.
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

        :param object_name: The S3 path of the file to compare.
        :param obj: The object data in bytes.
        :param raise_exception: If True, raises any exception that occurs, otherwise prints the error.
        :return: True if the file has changed (ETag does not match MD5 hash), False otherwise.
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
