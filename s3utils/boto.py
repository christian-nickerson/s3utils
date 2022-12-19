from botocore.client import Config
from pathlib import Path
import boto3
import os


class BotoFuncs:

    """S3 Filesystem read and write functions"""

    def __init__(self, ssl: bool) -> None:
        """S3 API functions from boto3.

        :param use_keys: Boolean flag to use keys and secret from env variables.
        :param ssl: Boolean flag to use SSL encryption.
        """
        self.ssl = ssl
        self.config = Config(connect_timeout=1000, retries={"max_attempts": 2}, signature_version="s3v4")
        self._connect_boto()

    def _connect_boto(self, config: Config) -> None:
        """Connect to S3 using boto3.

        :param config: Boto3 Config option
        """
        self.boto_client = boto3.client("s3", config=config, use_ssl=self.ssl)
        self.boto_resource = boto3.resource("s3", config=config, use_ssl=self.ssl)

    def upload_file(self, s3_bucket: str, s3_path: str, local_path: str) -> None:
        """Upload local file to S3.

        :param s3_bucket: Name of S3 bucket to upload to
        :param s3_path: S3 prefix to upload to
        :param local_path: Local path to upload file from
        """
        self.boto_client.upload_file(local_path, s3_bucket, s3_path)
        os.remove(local_path)

    def download_file(self, s3_bucket: str, s3_path: str, local_path: str) -> None:
        """Download file from S3 to local directory.

        :param s3_bucket: Name of S3 bucket to download from
        :param s3_path: S3 prefix to download from
        :param local_path: Local path to write file to
        """
        filename = Path(s3_path).name
        self.boto_resource.Bucket(s3_bucket).download_file(s3_path, os.path.join(local_path, filename))
