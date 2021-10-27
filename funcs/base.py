from abc import ABC, abstractmethod

class S3Base(ABC):
    
    @abstractmethod
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, endpoint_url: str, ssl: bool):
        """ Base init method required for all S3 sub-classes """
        pass

if __name__ == "__main__":
    pass