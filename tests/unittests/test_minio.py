from s3utils import S3
from testcontainer_python_minio import MinioContainer
from minio import Minio
from os.path import join
from os import environ
import unittest

class TestMinioFuncs(unittest.TestCase):

    minio = MinioContainer()
    bucket_name = 'test-bucket'
    fixtures_folder = 'tests/fixtures'
    test_json_name = 'test_json.json'
    test_csv_name = 'test_csv.csv'
    test_parquet_name = 'test_parquet.parquet.snappy'

    @classmethod
    def setUpClass(cls) -> None:
        cls.minio.start()
        environ['ENDPOINT_URL'] = cls.minio.get_url()
        environ['AWS_ACCESS_KEY_ID'] = cls.minio.accessKey()
        environ['AWS_SECRET_ACCESS_KEY'] = cls.minio.secretKey()

    def setUp(self) -> None:
        self.client = Minio(self.minio.get_url().replace("http://",""), 
                            self.minio.accessKey(), 
                            self.minio.secretKey(), 
                            secure = False)
        self.client.make_bucket(self.bucket_name)

    def tearDown(self) -> None:
        for obj in self.client.list_objects(self.bucket_name, recursive = True):
            self.client.remove_object(self.bucket_name, obj.object_name)
        self.client.remove_bucket(self.bucket_name)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.minio.stop()

    def test_ll(self) -> None:
        s3 = S3(use_keys = True, ssl = False)
        self.client.fput_object(self.bucket_name, self.test_csv_name, join(self.fixtures_folder, self.test_csv_name))
        self.client.fput_object(self.bucket_name, self.test_json_name, join(self.fixtures_folder, self.test_json_name))
        self.client.fput_object(self.bucket_name, self.test_parquet_name, join(self.fixtures_folder, self.test_parquet_name))
        s3_df = s3.ll(self.bucket_name)
        self.assertEqual(len(s3_df), 3)

if __name__ == "__main__":
    unittest.main()