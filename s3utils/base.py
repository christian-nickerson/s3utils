from abc import ABC, abstractmethod

class S3Base(ABC):
    
    @abstractmethod
    def __init__(self):
        """ Base init method required for all S3 sub-classes """
        pass

if __name__ == "__main__":
    pass