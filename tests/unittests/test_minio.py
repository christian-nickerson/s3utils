from moto import mock_s3
from funcs.minio import MinioFuncs
from testcontainer_python_minio import MinioContainer
from os.path import join
import unittest
import boto3
import os

bucket_name = 'test-bucket'
fixtures_folder = 'tests/fixtures'
test_json_name = 'test_json.json'
test_csv_name = 'test_csv.csv'
test_parquet_name = 'test_parquet.parquet.snappy'

class TestMinioFuncs(unittest.TestCase):

    minio = MinioContainer()

    def setUpClass(self) -> None:
        self.minio.start()
        self.minio.make_bucket(bucket_name)

    def setUp(self) -> None:
        self.s3 = MinioFuncs(    
            aws_access_key_id = self.minio.accessKey,
            aws_secret_access_key = self.minio.secretKey,
            endpoint_url = self.minio.get_url(),
            ssl = False
        )

    def tearDown(self) -> None:
        for obj in self.s3.minio.list_objects(bucket_name, recursive = True):
            self.s3.minio.remove_object(bucket_name, obj.object_name)
        self.s3.minio.remove_bucket(bucket_name)

    def test_ll(self) -> None:
        self.s3.minio.fput_object(bucket_name, test_csv_name, join(fixtures_folder, test_csv_name))
        self.s3.minio.fput_object(bucket_name, test_json_name, join(fixtures_folder, test_json_name))
        self.s3.minio.fput_object(bucket_name, test_parquet_name, join(fixtures_folder, test_parquet_name))
        s3_df = self.s3.ll(bucket_name)
        print(s3_df)

if __name__ == "__main__":
    unittest.main()