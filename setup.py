from setuptools import setup, find_packages

setup(
    name = 's3_utils',
    version = '0.1.0',
    url = 'https://github.com/christian-nickerson/s3_utils',
    author = 'Christian Nickerson',
    author_email = 'christian_nickerson@hotmail.com',
    packages = find_packages(exclude = ["*.tests","*.tests.*","tests.*","tests"]),
    description = 'Utility functions for working with data via the S3 API',
    install_requires = [
        's3fs>=0.5.2',
        'boto3==1.16.0',
        'pyarrow>=3.0.0',
        'pandas>=1.2.2',
        'minio>=7.0.2'
    ]
)
