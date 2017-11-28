from __future__ import print_function

from UNIX_like_ACL import *

class POSIX_ACL(UNIX_like_ACL):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(POSIX_ACL, self).__init__()
    
        print("init POSIX_ACL")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean POSIX_ACL")
        
        #super(self.__class__, self).__del__()
        #super(POSIX_ACL, self).__del__()
    
    # end of function __del__
    
    def get(self, path):
    
        pass
    
    # end of function get
    
    def set(self, path, privilege):
    
        pass
    
    # end of function set
    
    def modify(self, path, privilege):
    
        pass
    
    # end of function modify
    
    def remove(self, path, privilege):
    
        pass
    
    # end of function remove

# end of class POSIX_ACL

if __name__ == "__main__":

    obj = POSIX_ACL()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
