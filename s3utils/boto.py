from s3utils.base import S3Base
from botocore.client import Config
from pathlib import Path
import boto3
import os

class BotoFuncs(S3Base):

    def __init__(self, use_keys: bool, ssl: bool) -> None:
        """ S3 API functions from boto3.

        :param use_keys: Boolean flag to use keys and secret from env variables.
        :param ssl: Boolean flag to use SSL encryption.
        """
        self.ssl = ssl
        self.use_keys = use_keys
        self.config = Config(connect_timeout = 1000, retries = {'max_attempts': 2}, signature_version = 's3v4')
        self._connect_boto()
        super().__init__(use_keys, ssl)

    def _connect_boto(self) -> None:
        """ Connect to S3 using boto3. """
        self.boto_client = self._client()
        self.boto_resource = self._resource()

    def _resource(self) -> boto3.resource:
        """ Build resource object from boto3. """
        if not self.use_keys:
            return boto3.resource('s3', use_ssl = self.ssl)
        else: 
            return boto3.resource('s3',
                endpoint_url = os.getenv('ENDPOINT_URL'),
                aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
                config = self.config,
                use_ssl = self.ssl
            )

    def _client(self) -> boto3.client:
        """ Build resource object from boto3. """
        if not self.use_keys:
            return boto3.client('s3', use_ssl = self.ssl)
        else: 
            return boto3.client('s3',
                endpoint_url = os.getenv('ENDPOINT_URL'),
                aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
                config = self.config,
                use_ssl = self.ssl
            )

    def upload_file(self, s3_bucket: str, s3_path: str, local_path: str) -> None:
        """ Upload local file to S3.

        :param s3_bucket: Name of S3 bucket to upload to
        :param s3_path: S3 prefix to upload to
        :param local_path: Local path to upload file from
        """
        self.boto_client.upload_file(local_path, s3_bucket, s3_path)
        return os.remove(local_path)

    def download_file(self, s3_bucket: str, s3_path: str, local_path: str) -> None:
        """ Download file from S3 to local directory.

        :param s3_bucket: Name of S3 bucket to download from
        :param s3_path: S3 prefix to download from
        :param local_path: Local path to write file to
        """
        filename = Path(s3_path).name
        return self.boto_resource.Bucket(s3_bucket).download_file(s3_path, os.path.join(local_path, filename))

if __name__ == "__main__":
    pass