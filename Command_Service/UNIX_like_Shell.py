from __future__ import print_function

import paramiko

from SSH_Session import *
from File_Operation import *

class UNIX_like_Shell(File_Operation):

    def __init__(self):
    
        #super(self.__class__, self).__init__()
        super(UNIX_like_Shell, self).__init__()
    
        print("init UNIX_like_Shell")
    
    # end of function __init__
    
    def __del__(self):
    
        print("clean UNIX_like_Shell")
    
    # end of function __del__
    
    def launch(self,
        SSH_session,
        command): 
        
        print("run Command " + command + " via SSH Session " + SSH_session)
        #return SSH_session.executeCommand(command)
        
    # end of function launch

# end of class File_Operation

if __name__ == "__main__":

    obj = UNIX_like_Shell()
    
    print("unit test: " + type(obj).__name__)
    
# end of function __main__
