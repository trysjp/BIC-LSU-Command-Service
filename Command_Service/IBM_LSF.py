from __future__ import print_function

from LSF import *

class IBM_LSF(LSF):

    def __init__(self, shell, utilities, session):
    
        #super(self.__class__, self).__init__(shell, utilities, session)
        super(IBM_LSF, self).__init__(shell, utilities, session)
    
        print("init IBM_LSF")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean IBM_LSF")
    
    # end of function __del__

# end of class IBM_LSF

if __name__ == "__main__":

    obj = IBM_LSF("KORN SHELL", "GNU UTILITIES 8", "SSH SESSION")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
