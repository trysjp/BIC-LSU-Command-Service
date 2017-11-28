from __future__ import print_function

import abc

class Job_Control(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
    
        print("init Job_Control")
    
    # end of function __init__
    
    @abc.abstractmethod
    def submitJob():
        pass
    # end of abstract function submitJob
    
    @abc.abstractmethod
    def deleteJob():
        pass
    # end of abstract function deleteJob
    
    @abc.abstractmethod
    def showJobStatus():
        pass
    # end of abstract function showJobStatus

# end of class Job_Control

if __name__ == "__main__":

    obj = Job_Control()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
