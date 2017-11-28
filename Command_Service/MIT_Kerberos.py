from __future__ import print_function

from Kerberos import *

class MIT_Kerberos(Kerberos):

    def __init__(self):
    
        super(self.__class__, self).__init__()
        
        print("init MIT_Kerberos")
    
    # end of function __init__

# end of class MIT_Kerberos

if __name__ == "__main__":

    obj = MIT_Kerberos()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
