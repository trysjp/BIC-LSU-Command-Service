from __future__ import print_function

from Kerberos import *

class Heimdal_Kerberos(Kerberos):

    def __init__(self):
    
        super(self.__class__, self).__init__()
        
        print("init Heimdal_Kerberos")
    
    # end of function __init__

# end of class Heimdal_Kerberos

if __name__ == "__main__":

    obj = Heimdal_Kerberos()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
