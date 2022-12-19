from s3fs import S3FileSystem
from pandas.core.frame import DataFrame
from gzip import GzipFile
from typing import Callable
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import json
import os


class S3fsFuncs:

    """S3 Filesystem read and write functions"""

    def __init__(self, use_keys: bool, ssl: bool) -> None:
        """S3 API functions from S3FileSystem.

        :param use_keys: Boolean flag to use keys and secret from env variables.
        :param ssl: Boolean flag to use SSL encryption.
        """
        self.ssl = ssl
        self.use_keys = use_keys
        self._connect_s3fs()
        super().__init__(use_keys, ssl)

    def _connect_s3fs(self) -> None:
        """Connect to S3 using boto3."""
        if not self.use_keys:
            self.s3fs = S3FileSystem(anon=False, use_ssl=self.ssl)
        else:
            self.s3fs = S3FileSystem(
                anon=False,
                key=os.getenv("AWS_ACCESS_KEY_ID"),
                secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                client_kwargs={"endpoint_url": os.getenv("ENDPOINT_URL")},
                use_ssl=self.ssl,
            )

    def obj_exists(self, s3_bucket: str, s3_path: str) -> None:
        """Check if file exists.

        :param s3_bucket: Name of S3 bucket to check objects from
        :param s3_path: S3 prefix to check items from
        """
        full_path = os.path.join(s3_bucket, s3_path)
        return self.s3fs.exists(full_path)

    def read_csv(self, s3_bucket: str, s3_path: str, compression: str = None, **kwargs) -> DataFrame:
        """Read csv file from S3 object store.

        :param s3_bucket: Name of S3 bucket to read from
        :param s3_path: S3 prefix to read items from
        :param compression: File compression to read
        """
        with self.s3fs.open(os.path.join(s3_bucket, s3_path), "rb") as file:
            obj = GzipFile(fileobj=file) if compression == "gzip" else file
            return pd.read_csv(obj, **kwargs)

    def to_csv(self, df: DataFrame, s3_bucket: str, s3_path: str, **kwargs) -> None:
        """Write DataFrame to csv in S3 object store.

        :param df: DataFrame to write to S3
        :param s3_bucket: Name of S3 bucket to write to
        :param s3_path: S3 prefix to write to
        """
        with self.s3fs.open(os.path.join(s3_bucket, s3_path), "wb") as file:
            return df.to_csv(file, index=False, **kwargs)

    def read_parquet(self, s3_bucket: str, s3_path: str) -> DataFrame:
        """Read parquet file from S3 object store.

        :param s3_bucket: Name of S3 bucket to read from
        :param s3_path: S3 prefix to read items from
        """
        full_path = f"s3://{s3_bucket}/{s3_path}"
        dataset = pq.ParquetDataset(full_path, filesystem=self.s3fs)
        table = dataset.read()
        return table.to_pandas()

    def to_parquet(
        self,
        df: DataFrame,
        s3_bucket: str,
        s3_path: str,
        compression: str = "snappy",
        partition_cols: list = None,
        filename_cb: Callable = None,
        schema: pa.schema = None,
        preserve_index: bool = False,
    ) -> None:
        """Write DataFrame to parquet in S3 object store

        :param df: DataFrame to write to S3
        :param s3_bucket: Name of S3 bucket to write to
        :param s3_path: S3 prefix to write to
        :param compression: Compression type to write file with
        :param partition_cols: Columns in DataFrame to partition using
        :param filename_cb: Callback function to define file name
        :param schema: pyarrow schema to define file structure
        :param preserve_index: Preserve DataFrame index in parquet file or not
        """
        tbl = pa.Table.from_pandas(df, schema=schema, preserve_index=preserve_index)
        pq.write_to_dataset(
            table=tbl,
            root_path=os.path.join(s3_bucket, s3_path),
            partition_cols=partition_cols,
            partition_filename_cb=filename_cb,
            compression=compression,
            filesystem=self.s3fs,
            use_deprecated_int96_timestamps=True,
            coerce_timestamps="ms",
        )

    def read_json(self, s3_bucket: str, s3_path: str) -> dict:
        """Reads json and returns dict from S3

        :param s3_bucket: Name of bucket to read from.
        :param key: S3 path to read file from.
        """
        with self.s3fs.open(os.path.join(s3_bucket, s3_path), "r") as file:
            dict_file = json.load(file)
            file.close()
        return dict_file

    def to_json(self, dict: dict, s3_bucket: str, s3_path: str) -> None:
        """Reads json and returns dict from S3

        :param dict: Dictionary write to S3
        :param s3_bucket: Name of bucket to read from.
        :param s3_path: S3 path to read file from.
        """
        with self.s3fs.open(os.path.join(s3_bucket, s3_path), "w") as file:
            json.dump(dict, file, indent=4)
