from __future__ import print_function

from UNIX_like_Shell import *

class Tenex_C_Shell(UNIX_like_Shell):

    def __init__(self):
    
        super(self.__class__, self).__init__()
    
        print("init Tenex_C_Shell")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean Tenex_C_Shell")
    
    # end of function __del__
    
    def list(self, path):
        pass
    # end of function list (implementation of the abstract method)
    
    def rename(self, old_path, new_path):
        pass
    # end of abstract function rename (implementation of the abstract method)
    
    def remove(self, path):
        pass
    # end of abstract function remove (implementation of the abstract method)
    
    def copy(self, origin_path, copy_path):
        pass
    # end of abstract function copy (implementation of the abstract method)
    
    def move(self, source_path, destination_path):
        pass
    # end of abstract function move (implementation of the abstract method)
    
    def make_directory(self, path):
        pass
    # end of abstract function make_directory
    # (implementation of the abstract method)

# end of class Tenex_C_Shell

if __name__ == "__main__":

    obj = Tenex_C_Shell()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
