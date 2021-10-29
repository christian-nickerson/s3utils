from moto import mock_s3
from funcs.boto import BotoFuncs
from os import listdir, mkdir, remove
from os.path import isfile, isdir, join
import unittest
import boto3
import json

bucket_name = 'test-bucket'
test_data = {'hello': 'world'}
test_temp_folder = 'tests/temp'
test_json_name = 'test_json.json'

access_key = 'test_access_key_id'
secret_key = 'test_secret_access_key'

@mock_s3
class TestBotoFuncs(unittest.TestCase):

    def setUp(self) -> None:
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.resource.create_bucket(Bucket = bucket_name)
        self.bucket = self.resource.Bucket(bucket_name)
        if not isdir(test_temp_folder):
            mkdir(test_temp_folder)
        with open(join(test_temp_folder, test_json_name), 'w') as f:
            json.dump(test_data, f)

    def tearDown(self) -> None:
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()
        if isfile(join(test_temp_folder, test_json_name)): 
            remove(join(test_temp_folder, test_json_name))

    def test_file_uploaded(self) -> None:
        s3 = BotoFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        s3.upload_file(bucket_name, test_json_name, join(test_temp_folder, test_json_name))
        for key in self.bucket.objects.all():
            self.assertEqual(key.key, test_json_name)

    def test_uploaded_file_removed(self) -> None:
        s3 = BotoFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        s3.upload_file(bucket_name, test_json_name, join(test_temp_folder, test_json_name))
        local_files = [f for f in listdir(test_temp_folder) if isfile(join(test_temp_folder, f))]
        self.assertEqual(len(local_files), 0)

    def test_downloaded_file_exists(self) -> None:
        s3 = BotoFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        self.client.upload_file(join(test_temp_folder, test_json_name), bucket_name, test_json_name)
        s3.download_file(bucket_name, test_json_name, test_temp_folder)
        self.assertTrue(isfile(join(test_temp_folder, test_json_name)))

if __name__ == "__main__":
    unittest.main()
