from __future__ import print_function

import abc

class File_Operation(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
    
        print("init File_Operation")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean File_Operation")
    
    # end of function __del__
    
    @abc.abstractmethod
    def list():
        pass
    # end of abstract function list
    
    @abc.abstractmethod
    def rename():
        pass
    # end of abstract function rename
    
    @abc.abstractmethod
    def remove():
        pass
    # end of abstract function remove
    
    @abc.abstractmethod
    def copy():
        pass
    # end of abstract function copy
    
    @abc.abstractmethod
    def create():
        pass
    # end of abstract function create
    
    @abc.abstractmethod
    def getSize():
        pass
    # end of abstract function getSize
    
    @abc.abstractmethod
    def getACL():
        pass
    # end of abstract function getACL
    
    @abc.abstractmethod
    def setACL():
        pass
    # end of abstract function setACL
    
    @abc.abstractmethod
    def modifyACL():
        pass
    # end of abstract function modifyACL
    
    @abc.abstractmethod
    def removeACL():
        pass
    # end of abstract function removeACL
    
# end of class File_Operation
