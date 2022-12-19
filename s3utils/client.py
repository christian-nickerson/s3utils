from s3utils.boto import BotoFuncs
from s3utils.s3fs import S3fsFuncs


class S3(BotoFuncs, S3fsFuncs):

    """S3 utlility functions client."""

    def __init__(self, ssl: bool = False):
        """Functions for interacting with S3 data stores.

        :param ssl: Boolean flag to use SSL encryption.
        """
        super().__init__(ssl=ssl)
