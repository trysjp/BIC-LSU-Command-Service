from __future__ import print_function

from POSIX_Shell import *

class Almquist_Shell(POSIX_Shell):

    def __init__(self):
    
        super(self.__class__, self).__init__()
    
        print("init Almquist_Shell")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean Almquist_Shell")
    
    # end of function __del__

# end of class Almquist_Shell

if __name__ == "__main__":

    obj = Almquist_Shell()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
