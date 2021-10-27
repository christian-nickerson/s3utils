from funcs.base import *
from funcs.boto import *
from funcs.s3fs import *
from funcs.minio import *

class S3:

    def __init__(   self,
                    aws_access_key_id: str, 
                    aws_secret_access_key: str,
                    endpoint_url: str,
                    ssl: bool = False
                ):
        """ Functions for interacting with S3 data stores.

        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS access secret
        :param endpoint_url: S3 endpoint URL
        :param ssl: Boolean flag to use SSL encryption
        """
        self.registry = []
        self._compile()
        self._connect_s3(aws_access_key_id, aws_secret_access_key, endpoint_url, ssl)

    def _register_class(self, s3_class: S3Base) -> None:
        """ Factory register function for registering sub-classes.

        :param s3_class: s3 sub-class to be registered for compiling
        """
        self.registry.append(s3_class)

    def _connect_s3(self, aws_access_key_id: str, aws_secret_access_key: str, endpoint_url: str, ssl: bool) -> None:
        """ Factory intialiser for initialising sub-classes.

        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS access secret
        :param endpoint_url: S3 endpoint URL
        :param ssl: Boolean flag to use SSL encryption
        """
        for s3_class in self.registry:
            super(s3_class, self).__init__(
                aws_access_key_id = aws_access_key_id, 
                aws_secret_access_key = aws_secret_access_key, 
                endpoint_url = endpoint_url, 
                ssl = ssl
            )

    def _compile(self) -> None:
        """ Factory build function for compiling sub-classes into parent class.
        """
        self._register_class(BotoFuncs)
        self._register_class(S3fsFuncs)
        self._register_class(MinioFuncs)

if __name__ == '__main__':
    pass