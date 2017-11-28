from __future__ import print_function

import abc

class GUI(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
    
        print("init GUI")
    
    # end of function __init__
    
    @abc.abstractmethod
    def convertScriptToSkeleton():
        pass
    # end of abstract function convertScriptToSkeleton
    
    @abc.abstractmethod
    def insertInputToScript():
        pass
    # end of abstract function insertInputToScript

# end of class GUI

if __name__ == "__main__":

    obj = GUI()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
