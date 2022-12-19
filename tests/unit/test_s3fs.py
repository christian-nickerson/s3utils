from moto import mock_s3
from s3utils import S3
from os.path import join
import pandas as pd
import unittest
import boto3
import json


@mock_s3
class TestS3fsFuncs(unittest.TestCase):

    bucket_name = "test-bucket"
    fixtures_folder = "tests/fixtures"
    test_json_name = "test_json.json"
    test_csv_name = "test_csv.csv"
    test_parquet_name = "test_parquet.parquet.snappy"
    access_key = "test_access_key_id"
    secret_key = "test_secret_access_key"

    def setUp(self) -> None:
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.resource.create_bucket(Bucket=self.bucket_name)
        self.bucket = self.resource.Bucket(self.bucket_name)
        self.s3 = S3(ssl=False)

    def tearDown(self) -> None:
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()

    def test_gzip_csv_reads_writes_correctly(self) -> None:
        test_df = pd.read_csv(join(self.fixtures_folder, self.test_csv_name))
        test_df_rows = len(test_df.index)
        self.s3.to_csv(test_df, self.bucket_name, f"{self.test_csv_name}.gz", compression="gzip")
        s3_df = self.s3.read_csv(self.bucket_name, f"{self.test_csv_name}.gz", compression="gzip")
        self.assertEqual(len(s3_df.index), test_df_rows)

    def test_json_reads_writes_correctly(self) -> None:
        with open(join(self.fixtures_folder, self.test_json_name)) as f:
            test_dict = json.load(f)
        test_dict_len = len(test_dict)
        self.s3.to_json(test_dict, self.bucket_name, self.test_json_name)
        s3_dict = self.s3.read_json(self.bucket_name, self.test_json_name)
        self.assertEqual(len(s3_dict), test_dict_len)

    def test_snappy_parquet_reads_writes_correctly(self) -> None:
        test_df = pd.read_parquet(join(self.fixtures_folder, self.test_parquet_name))
        test_df_rows = len(test_df.index)
        self.s3.to_parquet(test_df, self.bucket_name, self.test_parquet_name)
        s3_df = self.s3.read_parquet(self.bucket_name, self.test_parquet_name)
        self.assertEqual(len(s3_df.index), test_df_rows)

    def test_object_exists(self) -> None:
        self.client.upload_file(join(self.fixtures_folder, self.test_csv_name), self.bucket_name, self.test_csv_name)
        self.assertTrue(self.s3.obj_exists(self.bucket_name, self.test_csv_name))


if __name__ == "__main__":
    unittest.main()
