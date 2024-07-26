import unittest
import boto3
import os
from moto import mock_aws
from handlers.s3_handler import S3Handler

class TestS3Handler(unittest.TestCase):

    @mock_aws
    def setUp(self):
        # Start mock AWS instance
        self.mock_aws = mock_aws()
        self.mock_aws.start()

        # Set up test resources
        self.bucket_name = 'my-test-bucket'
        self.s3_handler = S3Handler(bucket_name=self.bucket_name)
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.s3.create_bucket(Bucket=self.bucket_name)

    @mock_aws
    def tearDown(self):
        self.mock_aws.stop()

    @mock_aws
    def test_upload_file(self):
        file_name = 'test_file.txt'
        with open(file_name, 'w') as f:
            f.write('This is a test file.')

        self.s3_handler.upload_file(file_name)

        objects = self.s3.list_objects_v2(Bucket=self.bucket_name)['Contents']
        self.assertEqual(len(objects), 1)
        self.assertTrue(objects[0]['Key'].startswith('test_file.txt_'))

        # Clean up
        if os.path.exists(file_name):
            os.remove(file_name)

    @mock_aws
    def test_upload_object(self):
        object_data = b'This is a test object.'
        object_name = 'test_object.txt'

        self.s3_handler.upload_object(object_data, object_name)

        objects = self.s3.list_objects_v2(Bucket=self.bucket_name)['Contents']
        self.assertEqual(len(objects), 1)
        self.assertTrue(objects[0]['Key'].startswith('test_object.txt_'))

        # Clean up
        if os.path.exists('test_file.txt'):
            os.remove('test_file.txt')

    @mock_aws
    def test_download_file_to_ram(self):
        object_name = 'test_file.txt'
        object_data = b'This is a test file.'
        self.s3.put_object(Bucket=self.bucket_name, Key=object_name, Body=object_data)

        downloaded_data = self.s3_handler.download_file(object_name)
        self.assertEqual(downloaded_data, object_data)

        # Clean up
        if os.path.exists('test_file.txt'):
            os.remove('test_file.txt')

    @mock_aws
    def test_download_file_to_local(self):
        object_name = 'test_file.txt'
        object_data = b'This is a test file.'
        self.s3.put_object(Bucket=self.bucket_name, Key=object_name, Body=object_data)

        local_file_name = 'downloaded_test_file.txt'
        self.s3_handler.download_file(object_name, save_to_local=local_file_name)
        
        with open(local_file_name, 'rb') as f:
            downloaded_data = f.read()

        self.assertEqual(downloaded_data, object_data)

        # Clean up
        if os.path.exists(local_file_name):
            os.remove(local_file_name)

    @mock_aws
    def test_download_etag(self):
        object_name = 'test_file.txt'
        object_data = b'This is a test file.'
        self.s3.put_object(Bucket=self.bucket_name, Key=object_name, Body=object_data)

        etag = self.s3_handler.download_etag(object_name)
        expected_etag = self.s3.head_object(Bucket=self.bucket_name, Key=object_name)['ETag']

        self.assertEqual(etag, expected_etag)

        # Clean up
        if os.path.exists('test_file.txt'):
            os.remove('test_file.txt')

    @mock_aws
    def test_has_file_changed(self):
        object_name = 'test_file.txt'
        object_data = b'This is a test file.'
        self.s3.put_object(Bucket=self.bucket_name, Key=object_name, Body=object_data)

        # The object has not changed
        self.assertFalse(self.s3_handler.has_file_changed(object_name, object_data))

        # The object has changed
        new_object_data = b'This is a modified test file.'
        self.assertTrue(self.s3_handler.has_file_changed(object_name, new_object_data))

        # Clean up
        if os.path.exists('test_file.txt'):
            os.remove('test_file.txt')


if __name__ == '__main__':
    unittest.main()
