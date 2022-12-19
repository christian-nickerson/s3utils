from setuptools import setup, find_packages

setup(
    name="s3Utils",
    version="0.0.1",
    url="https://github.com/christian-nickerson/s3utils",
    author="Christian Nickerson",
    author_email="christian_nickerson@hotmail.com",
    packages=find_packages(),
    description="Utility functions for working with data via the S3 API",
    install_requires=[
        "s3fs~=0.4.2",
        "boto3~=1.19.6",
        "pyarrow==6.0.0",
        "pandas==1.3.4",
        "moto==2.2.11",
    ],
)
