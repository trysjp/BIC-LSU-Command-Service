from __future__ import print_function

from SSH_Session import *

class GSISSH(SSH_Session):

    def __init__(self):
    
        super(self.__class__, self).__init__()
    
        print("init GSISSH")
    
    # end of function __init__

# end of class GSISSH

if __name__ == "__main__":

    obj = GSISSH()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
