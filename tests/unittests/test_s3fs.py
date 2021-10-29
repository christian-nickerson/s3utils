from moto import mock_s3
from funcs.s3fs import S3fsFuncs
from os.path import join
import pandas as pd
import unittest
import boto3
import json

bucket_name = 'test-bucket'
fixtures_folder = 'tests/fixtures'
test_json_name = 'test_json.json'
test_csv_name = 'test_csv.csv'
test_parquet_name = 'test_parquet.parquet.snappy'

access_key = 'test_access_key_id'
secret_key = 'test_secret_access_key'

@mock_s3
class TestS3fsFuncs(unittest.TestCase):

    def setUp(self) -> None:
        self.resource = boto3.resource('s3')
        self.resource.create_bucket(Bucket = bucket_name)
        self.bucket = self.resource.Bucket(bucket_name)

    def tearDown(self) -> None:
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()

    def test_gzip_csv_reads_writes_correctly(self) -> None:
        s3 = S3fsFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        test_df = pd.read_csv(join(fixtures_folder, test_csv_name))
        test_df_rows = len(test_df.index)
        s3.to_csv(test_df, bucket_name, f'{test_csv_name}.gz', compression = 'gzip')
        s3_df = s3.read_csv(bucket_name, f'{test_csv_name}.gz', compression = 'gzip')
        self.assertEqual(len(s3_df.index), test_df_rows)

    def test_json_reads_writes_correctly(self) -> None:
        s3 = S3fsFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        with open(join(fixtures_folder, test_json_name)) as f:
            test_dict = json.load(f)
        test_dict_len = len(test_dict)
        s3.to_json(test_dict, bucket_name, test_json_name)
        s3_dict = s3.read_json(bucket_name, test_json_name)
        self.assertEqual(len(s3_dict), test_dict_len)

    def test_snappy_parquet_reads_writes_correctly(self) -> None:
        s3 = S3fsFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        test_df = pd.read_parquet(join(fixtures_folder, test_parquet_name))
        test_df_rows = len(test_df.index)
        s3.to_parquet(test_df, bucket_name, test_parquet_name)
        s3_df = s3.read_parquet(bucket_name, test_parquet_name)
        self.assertEqual(len(s3_df.index), test_df_rows)

    def test_object_exists(self) -> None:
        s3 = S3fsFuncs( aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        endpoint_url = None,
                        ssl = False)
        df = pd.read_csv(join(fixtures_folder, test_csv_name))
        s3.to_csv(df, bucket_name, test_csv_name)
        self.assertTrue(s3.obj_exists(bucket_name, test_csv_name))

if __name__ == "__main__":
    unittest.main()