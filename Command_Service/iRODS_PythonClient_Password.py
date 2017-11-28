from __future__ import print_function

from Authentication import *

class iRODS_PythonClient_Password(Authentication):

    def __init__(self):
    
        super(self.__class__, self).__init__()
    
        print("init iRODS_PythonClient_Password")
    
    # end of function __init__

# end of class iRODS_PythonClient_Password

if __name__ == "__main__":

    obj = iRODS_PythonClient_Password()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__()
