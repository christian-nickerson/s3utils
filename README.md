![Python CI Testing](https://github.com/christian-nickerson/s3utils/actions/workflows/python-ci.yml/badge.svg)

# S3Utils
Utility functions for reading, writing and working with data in S3 data sotres via the S3 API.

# Contents
1. [Install Guide](#install)
2. [Getting Started](#getting_started)

## Install Guide <a name="install"></a>
To install S3Utils, open the terminal and run the following:
``` zsh
cd YOUR/PROJECT/DIRECTORY

pip install git+https://github.com/christian-nickerson/s3utils
```

## Getting Started <a name="getting_started"></a>
S3Utils can either use inherited IAM roles for autheticating against S3.

To use with IAM roles:
``` python
from s3utils import S3
s3 = S3(ssl = True)
``` 

To download a file from an S3 data store to your local directory:
``` python
bucket_name = 'YOUR-BUCKET-NAME'
key = 'YOUR/OBJECT/KEY'
local_path = 'YOUR/LOCAL/PATH'

s3.download_file(bucket_name, key, local_path)
```

To read a parquet file directly from S3 data store:
``` python
bucket_name = 'YOUR-BUCKET-NAME'
key = 'YOUR/OBJECT/KEY'

df = s3.read_parquet(bucket_name, key)
```

To write a csv from a dataframe directly to S3:
``` python
df = DataFrame() # replace with your DataFrame to be written
bucket_name = 'YOUR-BUCKET-NAME'
key = 'YOUR/OBJECT/KEYFILENAME.csv'

s3.to_csv(df, bucket_name, key)
```