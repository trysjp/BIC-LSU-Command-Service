from __future__ import print_function

from PBS import *

class OpenPBS(PBS):

    def __init__(self, shell, utilities, session):
    
        #super(self.__class__, self).__init__(shell, utilities, session)
        super(OpenPBS, self).__init__(shell, utilities, session)
    
        print("init OpenPBS")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean OpenPBS")
    
    # end of function __del__

# end of class OpenPBS

if __name__ == "__main__":

    obj = OpenPBS("KORN SHELL", "GNU UTILITIES 8", "SSH SESSION")
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
