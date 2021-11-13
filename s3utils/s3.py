from s3utils.base import *
from s3utils.boto import *
from s3utils.s3fs import *
from s3utils.minio import *

class S3(   BotoFuncs,
            S3fsFuncs,
            MinioFuncs
        ):

    def __init__(self, use_keys: bool = False, ssl: bool = False):
        """ Functions for interacting with S3 data stores.

        :param use_keys: Boolean flag to use keys and secret from env variables.
        :param ssl: Boolean flag to use SSL encryption.
        """
        super().__init__(use_keys, ssl)

if __name__ == '__main__':
    pass