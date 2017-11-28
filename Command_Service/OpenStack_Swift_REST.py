from __future__ import print_function

from File_Operation import *

class OpenStack_Swift_REST(File_Operation):

    def __init__(self):
    
        super(self.__class__, self).__init__()
    
        print("init OpenStack_Swift_REST")
    
    # end of function __init__

# end of class OpenStack_Swift_REST

if __name__ == "__main__":

    obj = OpenStack_Swift_REST()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
