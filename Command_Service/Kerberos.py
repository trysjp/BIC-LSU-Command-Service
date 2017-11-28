from __future__ import print_function

from Authentication import *

class Kerberos(Authentication):

    def __init__(self):
    
        super(self.__class__, self).__init__()
        
        print("init Kerberos")
    
    # end of function __init__

# end of class Kerberos

if __name__ == "__main__":

    obj = Kerberos()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
