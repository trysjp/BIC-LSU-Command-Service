from __future__ import print_function

from Bourne_Shell import *

class Bourne_Shell_Cygwin(Bourne_Shell):

    def __init__(self):
    
        super(self.__class__, self).__init__()
    
        print("init Bourne_Shell_Cygwin")
    
    # end of function __init__

# end of class Bourne_Shell_Cygwin

if __name__ == "__main__":

    obj = Bourne_Shell_Cygwin()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
