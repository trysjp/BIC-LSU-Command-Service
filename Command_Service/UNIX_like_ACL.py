from __future__ import print_function

class UNIX_like_ACL(object):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(UNIX_like_ACL, self).__init__()
    
        print("init UNIX_like_ACL")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean UNIX_like_ACL")
        
        #super(self.__class__, self).__del__()
        #super(UNIX_like_ACL, self).__del__()
    
    # end of function __del__

# end of class UNIX_like_ACL

if __name__ == "__main__":

    obj = UNIX_like_ACL()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
