# S3 Utils
Utility functions for reading, writing and working with data in S3 data sotres via the S3 API.

# Contents
1. [Install Guide](#install)
2. [Getting Started](#getting_started)
3. [Project Structure](#structure)

## Install Guide <a name="install"></a>
1. Open the Terminal and run the following (replace paths with your own):
    ``` bash
    $ cd YOUR/PROJECT/DIRECTORY

    $ git submodule add https://github.com/christian-nickerson/s3_utils s3_utils
    ```

2. Create and insitialise virtual environment:
    ``` bash
    $ python3 -m venv env

    $ source env/bin/activate
    ```

3. Pip install library into your virtual environment:
    ``` bash
    $ pip install s3_utils/
    ```

## Getting Started <a name="getting_started"></a>
to Instantiate the class:
``` python
from s3 import S3

aws_access_key_id = 'xxxx-xxxx-xxxx-xxxx'
aws_secret_access_key = 'xxxx-xxxx-xxxx-xxxx'
endpoint_url = 'https://YOUR-S3-URL'

s3 = S3(   
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    endpoint_url = endpoint_url
)
``` 

To download a file from an S3 data store to your local directory:
``` python
s3_bucket = 'YOUR-BUCKET-NAME'
s3_path = 'YOUR/BUCKET/FILE/PATH'
local_path = 'YOUR/LOCAL/PATH'

s3.download_file(s3_bucket, s3_path, local_path)
```

To read a parquet file directly from S3 data store:
``` python
s3_bucket = 'YOUR-BUCKET-NAME'
s3_path = 'YOUR/BUCKET/FILE/PATH'

df = s3.read_parquet(s3_bucket, s3_path)
```

To write a csv from a dataframe directly to S3:
``` python
df = DataFrame() # replace with your DataFrame to be written
s3_bucket = 'YOUR-BUCKET-NAME'
s3_path = 'YOUR/BUCKET/FILE/PATH/FILENAME.csv'

s3.to_csv(df, s3_bucket, s3_path)
```

## Project Structure <a name="structure"></a>
The project is built on the factory design pattern. The ```s3.py``` is the contains the factory class. The base and sub-classes are under the ```funcs/``` folder.

To extend the class:

1. Create a new sub-class file:
    ``` bash
    $ touch funcs/NEW-CLASS.py
    ```
2. Import the base class within the new class file to extend:
    ``` python
    from funcs.base import S3Base

    class NewClassFuncs(S3Base):

        def __init__(   self, 
                        aws_access_key_id: str, 
                        aws_secret_access_key: str, 
                        endpoint_url: str, 
                        ssl: bool
                    ) -> None:
            """ Connect to S3 using new fucntions.

            :param aws_access_key_id: AWS access key ID
            :param aws_secret_access_key: AWS access secret
            :param endpoint_url: S3 endpoint URL
            :param ssl: Boolean flag to use SSL encryption
            """
    ```

3. Once extended, edit the ```s3.py``` file to compile new class to parent class:
    ``` python
    from funcs.base import *
    from funcs.boto import *
    from funcs.s3fs import *
    from funcs.minio import *
    # import new class
    from funcs.NEW-CLASS import *
    ```
    ``` python
    def _compile(self) -> None:
        """ Factory build function for compiling sub-classes into parent class.
        """
        self._register_class(BotoFuncs)
        self._register_class(S3fsFuncs)
        self._register_class(MinioFuncs)
        # register new class
        self._register_class(NewClassFuncs)
    ```