from funcs.base import S3Base
from minio import Minio
from pandas.core.frame import DataFrame
import pandas as pd

class MinioFuncs(S3Base):

    def __init__(   self, 
                    aws_access_key_id: str, 
                    aws_secret_access_key: str, 
                    endpoint_url: str, 
                    ssl: bool
                ) -> None:
        """ Connect to MinIO using Minio fucntions.

        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS access secret
        :param endpoint_url: S3 endpoint URL
        :param ssl: Boolean flag to use SSL encryption
        """
        self.minio = Minio( endpoint = endpoint_url.replace("http://",""),
                            access_key = aws_access_key_id,
                            secret_key = aws_secret_access_key,
                            secure = ssl
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