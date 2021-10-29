from moto import mock_s3
from funcs.minio import MinioFuncs
from testcontainer_python_minio import MinioContainer
from minio import Minio
from os.path import join
import unittest

bucket_name = 'test-bucket'
fixtures_folder = 'tests/fixtures'
test_json_name = 'test_json.json'
test_csv_name = 'test_csv.csv'
test_parquet_name = 'test_parquet.parquet.snappy'

class TestMinioFuncs(unittest.TestCase):

    minio = MinioContainer()

    def setUp(self) -> None:
        self.minio.start()
        self.client = Minio(self.minio.get_url().replace("http://",""), 
                            self.minio.accessKey(), 
                            self.minio.secretKey(), 
                            secure = False)
        self.client.make_bucket(bucket_name)

    def tearDown(self) -> None:
        for obj in self.client.list_objects(bucket_name, recursive = True):
            self.client.remove_object(bucket_name, obj.object_name)
        self.client.remove_bucket(bucket_name)
        self.minio.stop()

    def test_ll(self) -> None:
        s3 = MinioFuncs(aws_access_key_id = self.minio.accessKey(),
                        aws_secret_access_key = self.minio.secretKey(),
                        endpoint_url = self.minio.get_url(),
                        ssl = False)
        s3.minio.fput_object(bucket_name, test_csv_name, join(fixtures_folder, test_csv_name))
        s3.minio.fput_object(bucket_name, test_json_name, join(fixtures_folder, test_json_name))
        s3.minio.fput_object(bucket_name, test_parquet_name, join(fixtures_folder, test_parquet_name))
        s3_df = s3.ll(bucket_name)
        self.assertEqual(len(s3_df), 3)

if __name__ == "__main__":
    unittest.main()