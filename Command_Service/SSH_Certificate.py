from __future__ import print_function

from Authentication import *

class SSH_Certificate(Authentication):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(SSH_Certificate, self).__init__()
    
        print("init SSH_Certificate")
    
    # end of function __init__

# end of class SSH_Certificate

if __name__ == "__main__":

    obj = SSH_Certificate()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
