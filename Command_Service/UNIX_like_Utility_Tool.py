from __future__ import print_function

class UNIX_like_Utility_Tool(object):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(UNIX_like_Utility_Tool, self).__init__()
    
        print("init UNIX_like_Utility_Tool")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean UNIX_like_Utility_Tool")
    
    # end of function __del__

# end of class File_Operation

if __name__ == "__main__":

    obj = UNIX_like_Utility_Tool()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
