from funcs.base import S3Base
from botocore.client import Config
from pathlib import Path
import boto3
import os

class BotoFuncs(S3Base):

    def __init__(   self, 
                    aws_access_key_id: str, 
                    aws_secret_access_key: str, 
                    endpoint_url: str, 
                    ssl: bool
                ) -> None:
        """ Connect to S3 using boto3 functions.

        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS access secret
        :param endpoint_url: S3 endpoint URL
        :param ssl: Boolean flag to use SSL encryption
        """
        config = Config(connect_timeout = 1000, retries = {'max_attempts': 2}, signature_version = 's3v4')
        self.boto = boto3.resource( 's3',
                                    endpoint_url = endpoint_url,
                                    aws_access_key_id = aws_access_key_id,
                                    aws_secret_access_key = aws_secret_access_key,
                                    config = config,
                                    use_ssl = ssl
                                )
        self.boto_client = boto3.client('s3',
                                        endpoint_url = endpoint_url,
                                        aws_access_key_id = aws_access_key_id,
                                        aws_secret_access_key = aws_secret_access_key,
                                        config = config,
                                        use_ssl = ssl
                                    )

    def upload_file(self, s3_bucket: str, s3_path: str, local_path: str) -> None:
        """ Upload local file to S3.

        :param s3_bucket: Name of S3 bucket to upload to
        :param s3_path: S3 prefix to upload to
        :param local_path: Local path to upload file from
        """
        self.boto.Bucket(s3_bucket).upload_file(s3_path, local_path)
        return os.remove(local_path)

    def download_file(self, s3_bucket: str, s3_path: str, local_path: str) -> None:
        """ Download file from S3 to local directory.

        :param s3_bucket: Name of S3 bucket to download from
        :param s3_path: S3 prefix to download from
        :param local_path: Local path to write file to
        """
        filename = Path(s3_path).name
        return self.boto.Bucket(s3_bucket).download_file(s3_path, os.path.join(local_path, filename))

if __name__ == "__main__":
    pass