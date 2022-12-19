from moto import mock_s3
from s3utils import S3
from os import listdir, remove
from os.path import isfile, join
import unittest
import boto3
import json


@mock_s3
class TestBotoFuncs(unittest.TestCase):

    bucket_name = "test-bucket"
    test_dict = {"hello": "world"}
    fixtures_folder = "tests/fixtures"
    json_name = "test_json.json"
    access_key = "test_access_key_id"
    secret_key = "test_secret_access_key"

    def setUp(self) -> None:
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.resource.create_bucket(Bucket=self.bucket_name)
        self.bucket = self.resource.Bucket(self.bucket_name)
        self.s3 = S3(ssl=False)
        with open(join(self.fixtures_folder, self.json_name), "w") as f:
            json.dump(self.test_dict, f)

    def tearDown(self) -> None:
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()
        with open(join(self.fixtures_folder, self.json_name), "w") as f:
            json.dump(self.test_dict, f)

    def test_file_uploaded(self) -> None:
        self.s3.upload_file(self.bucket_name, self.json_name, join(self.fixtures_folder, self.json_name))
        for key in self.bucket.objects.all():
            self.assertEqual(key.key, self.json_name)

    def test_uploaded_file_removed(self) -> None:
        self.s3.upload_file(self.bucket_name, self.json_name, join(self.fixtures_folder, self.json_name))
        local_files = [f for f in listdir(self.fixtures_folder) if isfile(join(self.fixtures_folder, self.json_name))]
        self.assertEqual(len(local_files), 0)

    def test_downloaded_file_exists(self) -> None:
        self.client.upload_file(join(self.fixtures_folder, self.json_name), self.bucket_name, self.json_name)
        remove(join(self.fixtures_folder, self.json_name))
        self.s3.download_file(self.bucket_name, self.json_name, self.fixtures_folder)
        self.assertTrue(isfile(join(self.fixtures_folder, self.json_name)))


if __name__ == "__main__":
    unittest.main()
