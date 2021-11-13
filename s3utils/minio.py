from s3utils.base import S3Base
from minio import Minio
from pandas.core.frame import DataFrame
from os import getenv
import pandas as pd

class MinioFuncs(S3Base):

    def __init__(self, use_keys: bool, ssl: bool) -> None:
        """ MinIO API functions from Minio.

        :param use_keys: Boolean flag to use keys and secret from env variables.
        :param ssl: Boolean flag to use SSL encryption.
        """
        self.ssl = ssl
        self.use_keys = use_keys
        self._connect_minio()
        super().__init__()

    def _connect_minio(self) -> None:
        """ Connect to S3 using boto3. """
        if not self.use_keys:
            self.minio = Minio('s3.amazonaws.com', secure = self.ssl)
        else: 
            self.minio = Minio( 
                endpoint = getenv('ENDPOINT_URL').replace("http://",""),
                access_key = getenv('AWS_ACCESS_KEY_ID'),
                secret_key = getenv('AWS_SECRET_ACCESS_KEY'),
                secure = self.ssl
            )

    def ll(self, s3_bucket: str, s3_path: str = None) -> DataFrame:
        """ Create a DataFrame of items in MinIO.

        :param s3_bucket: Name of MinIO bucket to list objects from
        :param s3_path: MinIO prefix to list items from
        """
        obj_list = []
        for obj in self.minio.list_objects(s3_bucket, prefix = s3_path, recursive = True):
            obj_list.append((   obj.bucket_name, 
                                obj.object_name, 
                                obj.last_modified, 
                                obj.etag, 
                                obj.size, 
                                obj.content_type
                            ))
        df = pd.DataFrame(obj_list, columns = ('s3_bucket', 's3_path', 'last_modified', 'etag', 'size', 'content_type'))
        df['last_modified'] = pd.to_datetime(df['last_modified'], errors = 'coerce', utc = True)
        return df

if __name__ == "__main__":
    pass